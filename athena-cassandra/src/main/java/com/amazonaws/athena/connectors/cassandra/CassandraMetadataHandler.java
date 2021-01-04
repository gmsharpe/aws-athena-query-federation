/*-
 * #%L
 * athena-cassandra
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.cassandra;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.athena.connectors.cassandra.connection.CassandraSessionFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.cassandra.CassandraMetadataHandler.SchemaColumns.*;
import static com.amazonaws.athena.connectors.cassandra.CassandraMetadataHandler.SchemaTables.COLUMNS;
import static com.amazonaws.athena.connectors.cassandra.CassandraMetadataHandler.SchemaTables.TABLES;
import static com.amazonaws.athena.connectors.cassandra.CassandraToArrowUtils.*;

public class CassandraMetadataHandler extends MetadataHandler
{

    public static final String BLOCK_PARTITION_SCHEMA_COLUMN_NAME = "partition_schema_name";
    public static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";

    // partition column metadata is retrieved from the system_schema.columns table.
    static final String PARTITION_COLUMN_NAME = "column_name";

    public static final String ALL_PARTITIONS = "*";
    private final CassandraSessionFactory cassandraSessionFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataHandler.class);

    private static final String SOURCE_TYPE = "cassandra";

    // todo - this is from the mysql metadata handler.  assess.
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    // todo - can we get this from DataStax ColumnDefinitions
    static final class SchemaColumns {
        static final String CLUSTERING_ORDER = "clustering_order";
        static final String TYPE = "type";
        static final String COLUMN_NAME = "column_name";
        static final String KIND = "kind";
        static final String POSITION = "position";
        static final String KEYSPACE_NAME = "keyspace_name";
        static final String TABLE_NAME = "table_name";
        static final String PARTITION_KEY = "partition_key";
        static final String CLUSTERING_KEY = "clustering_key";
    }

    static final class SchemaTables {
        public static final String TABLES = "tables";
        public static final String COLUMNS = "columns";

    }

    @VisibleForTesting
    public CassandraMetadataHandler(EncryptionKeyFactory encryptionKeyFactory,
                                    AWSSecretsManager secretsManager,
                                    AmazonAthena athena,
                                    String spillBucket,
                                    String spillPrefix,
                                    CassandraSessionConfig cassandraSessionConfig)
    {
        super(encryptionKeyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.cassandraSessionFactory =  Validate.notNull(CassandraSessionFactory.getConnectionFactory(cassandraSessionConfig),
                                                         "cassandraSessionFactory must not be null");
    }

    /**
     * Will attempt to retrieve all necessary CassandraSessionConfig values from System.getEnv()
     */
    public CassandraMetadataHandler()
    {
        super(SOURCE_TYPE);
        cassandraSessionFactory = CassandraSessionFactory.getConnectionFactory(CassandraSessionConfig.buildSessionConfigFromEnv());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        LOGGER.info("doListSchemaNames: enter {}", request.getCatalogName());
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            ResultSet resultSet = cqlSession.execute(String.format("SELECT %s FROM system_schema.columns;", KEYSPACE_NAME));
            Set<String> keyspaces = resultSet.all()
                                             .stream()
                                             .map(row -> row.getString(KEYSPACE_NAME))
                                             .collect(Collectors.toSet());

            LOGGER.debug(String.format("discovered the following keyspaces: [ %s ] ",
                         String.join(", ", keyspaces)));

            return new ListSchemasResponse(request.getCatalogName(), keyspaces);
        }

    }

    /**
     * https://stackoverflow.com/questions/38696316/how-to-list-all-cassandra-tables
     * https://docs.datastax.com/en/pdf/osscql3x.pdf
     *
     * @param allocator         Tool for creating and managing Apache Arrow Blocks.
     * @param listTablesRequest Provides details on who made the request and which Athena catalog and database they are querying.
     * @return
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator,
                                           ListTablesRequest listTablesRequest)
    {
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            LOGGER.info("{}: List table names for Catalog {}, Table {}",
                        listTablesRequest.getQueryId(),
                        listTablesRequest.getCatalogName(),
                        listTablesRequest.getSchemaName());
            return new ListTablesResponse(listTablesRequest.getCatalogName(),
                                          listTables(cqlSession, listTablesRequest.getSchemaName()));
        }
    }

    private Collection<TableName> listTables(CqlSession cqlSession, String schemaName)
    {
        ResultSet resultSet = cqlSession.execute(String.format("SELECT * FROM system_schema.%s", TABLES));

        return Collections.unmodifiableCollection(
                resultSet.all()
                         .stream()
                         .filter( row -> {
                             if(schemaName != null){
                                 String keyspace = row.getString(KEYSPACE_NAME);
                                 return (keyspace != null) ? keyspace.equals(schemaName) : false;
                             }
                             else{
                                 return true;
                             }
                         })
                         .map(row -> new TableName(row.getString(KEYSPACE_NAME), row.getString(TABLE_NAME)))
                         .collect(Collectors.toList()));
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        LOGGER.info("doGetTable: enter {}", request.getTableName());
        try (CqlSession cassandraCqlSession = cassandraSessionFactory.getSession()) {
            Schema schema = getTableSchema(cassandraCqlSession, request.getTableName(), null);
            GetTableResponse getTableResponse = new GetTableResponse(request.getCatalogName(),
                                                                     request.getTableName(),
                                                                     schema,
                                                                     Collections.emptySet());  // todo - should this be used at all?  could remove param altogether
            return getTableResponse;
        }
        catch (Exception exception) {
            throw new RuntimeException(exception);  // todo
        }
    }

    /**
     * @param cqlSession
     * @param tableName
     * @param partitionSchema
     * @return
     */
    Schema getTableSchema(CqlSession cqlSession, TableName tableName, Schema partitionSchema)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        String schemaQuery = String.format("SELECT * FROM system_schema.%s WHERE %s = :keyspace_name and %s = :table_name;",
                                           COLUMNS,
                                           KEYSPACE_NAME,
                                           TABLE_NAME);

        SimpleStatement statement = new SimpleStatementBuilder(schemaQuery).build();

        PreparedStatement preparedStatement = cqlSession.prepare(statement);

        BoundStatement bndStmt = preparedStatement.bind()
                                                  .setString(KEYSPACE_NAME, tableName.getSchemaName())
                                                  .setString(TABLE_NAME, tableName.getTableName());

        ResultSet resultSet = cqlSession.execute(bndStmt);

        resultSet.all().forEach(row -> {
            String cassandraType = row.getString(TYPE);
            String columnName = row.getString(COLUMN_NAME);
            ArrowType columnType = null;

            if(cassandraType != null) {
                columnType = getArrowTypeForCassandraField(new CassandraFieldInfo(columnName, cassandraType));
            }
            else {
                LOGGER.error(String.format("getSchema: Unable to map type for column[%s].  'type' was null in schema.",
                                           columnName));
            }

            if (columnType != null && SupportedTypes.isSupported(columnType)) {
                schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
            }
            else {
                LOGGER.error(String.format("getSchema: Unable to map type for column[%s] to a supported type, attempted %s",
                                           columnName,
                                           columnType));
            }
        });

        // add partition columns
        //partitionSchema.getFields().forEach(schemaBuilder::addField);

        return schemaBuilder.build();
    }

    /**
     * @param blockWriter           Used to write rows (partitions) into the Apache Arrow response.
     * @param getTableLayoutRequest
     * @param queryStatusChecker    A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker)
    {
       /* TableName tableName = getTableLayoutRequest.getTableName();

        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(),
                    tableName.getTableName());

        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {

            String tableSchemaSql =
                    String.format("SELECT * FROM system_schema.%s WHERE %s = :keyspace_name and %s = :table_name;", // and %s = :kind ALLOW FILTERING
                                  COLUMNS,
                                  KEYSPACE_NAME,
                                  TABLE_NAME);
            //,KIND);

            SimpleStatement statement = new SimpleStatementBuilder(tableSchemaSql).build();

            PreparedStatement preparedStatement = cqlSession.prepare(statement);

            // get partition_keys
            ResultSet resultSet = cqlSession.execute(preparedStatement.bind()
                                                                      .setString(KEYSPACE_NAME, tableName.getSchemaName())
                                                                      .setString(TABLE_NAME, tableName.getTableName()));
                                                                      //.setString(KIND, PARTITION_KEY));

            List<Row> partitionKeyRows = resultSet.all()
                                                  .stream()
                                                  .filter(row -> row.getString(KIND).equals(PARTITION_KEY))
                                                  .collect(Collectors.toList());

            Iterator<Row> rows = partitionKeyRows.iterator();

            // Return a single partition if no partitions defined
            if (!rows.hasNext()) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    LOGGER.info("Adding partition {}", ALL_PARTITIONS);
                    //we wrote 1 row so we return 1
                    return 1;
                });
            } else {
                do {
                    final String partitionName = rows.next().getString(PARTITION_COLUMN_NAME);

                    // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                    // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                        LOGGER.info("Adding partition {}", partitionName);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                while (rows.hasNext() && queryStatusChecker.isQueryRunning());
            }

            // get clustering_keys
            ResultSet clusteringKeysResultSet = cqlSession.execute(preparedStatement.bind()
                                                                                    .setString(KEYSPACE_NAME, tableName.getSchemaName())
                                                                                    .setString(TABLE_NAME, tableName.getTableName())
                                                                                    .setString(KIND, CLUSTERING_KEY));

            Iterator<Row> clusteringKeys = clusteringKeysResultSet.iterator();

            // Return a single partition if no partitions defined
            do {
                final String partitionName = clusteringKeys.next().getString(PARTITION_COLUMN_NAME);

                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                    LOGGER.info("Adding partition {}", partitionName);
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
            while (clusteringKeys.hasNext() && queryStatusChecker.isQueryRunning());
        }*/
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest getSplitsRequest) throws Exception
    {
        /*LOGGER.info("{}: Catalog {}, table {}",
                    getSplitsRequest.getQueryId(),
                    getSplitsRequest.getTableName().getSchemaName(),
                    getSplitsRequest.getTableName().getTableName());

        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);

            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

            LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                                              .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition));
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);*/

        // todo - evaluate - from DocDBMetadataHandler
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

        //Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(),
                                     Split.newBuilder(spillLocation, makeEncryptionKey())
                                          .build());
    }

/*    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                                                   .addField(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                                                   .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }*/
}
