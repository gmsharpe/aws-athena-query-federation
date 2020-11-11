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
package com.amazonaws.connectors.athena.cassandra;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraCredentialProvider;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionFactory;
import com.amazonaws.connectors.athena.cassandra.connection.StaticCassandraCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraMetadataHandler extends MetadataHandler
{

    public static final String BLOCK_PARTITION_SCHEMA_COLUMN_NAME = "partition_schema_name";
    public static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";

    static final String PARTITION_COLUMN_NAME = "partition_name";

    public static final String ALL_PARTITIONS = "*";
    private final CassandraSessionFactory cassandraSessionFactory;
    private final CassandraSessionConfig cassandraSessionConfig;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataHandler.class);

    private static final String SOURCE_TYPE = "cassandra";

    protected CassandraMetadataHandler(final CassandraSessionConfig cassandraSessionConfig,
                                       final CassandraSessionFactory cassandraSessionFactory)
    {
        super(SOURCE_TYPE);
        this.cassandraSessionFactory = Validate.notNull(cassandraSessionFactory,
                                                        "cassandraSessionFactory must not be null");
        this.cassandraSessionConfig = Validate.notNull(cassandraSessionConfig,
                                                       "cassandraSessionConfig must not be null");
    }

    public CassandraMetadataHandler(EncryptionKeyFactory encryptionKeyFactory,
                                    AWSSecretsManager secretsManager,
                                    AmazonAthena athena,
                                    String spillBucket,
                                    String spillPrefix)
    {
        super(encryptionKeyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);

        cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
        cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();
    }

    public CassandraMetadataHandler()
    {
        super(SOURCE_TYPE);
        cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
        cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();
    }

    public CassandraMetadataHandler(CassandraSessionConfig cassandraSessionConfig,
                                    CassandraSessionFactory cassandraSessionFactory,
                                    AWSSecretsManager secretsManager,
                                    AmazonAthena athena)
    {

        // todo can/should null values be passed here?
        super(null, secretsManager, athena, SOURCE_TYPE, null, null);
        this.cassandraSessionFactory = Validate.notNull(cassandraSessionFactory,
                                                        "cassandraSessionFactory must not be null");
        this.cassandraSessionConfig = Validate.notNull(cassandraSessionConfig,
                                                       "cassandraSessionConfig must not be null");
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        LOGGER.info("doListSchemaNames: enter", request.getCatalogName());
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            ResultSet resultSet = cqlSession.execute("SELECT DISTINCT keyspace_name FROM system_schema.columns;");
            List<String> keyspaces = resultSet.all().stream().map(row -> row.getString("keyspace_name")).collect(Collectors.toList());
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

        try (CqlSession cqlSession = cassandraSessionFactory.getSession(getCredentialProvider())) {
            LOGGER.info("{}: List table names for Catalog {}, Table {}", listTablesRequest.getQueryId(),
                        listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
            return new ListTablesResponse(listTablesRequest.getCatalogName(),
                                          listTables(cqlSession, listTablesRequest.getSchemaName()));
        }
    }

    private Collection<TableName> listTables(CqlSession cqlSession, String schemaName)
    {
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables");

        return Collections.unmodifiableCollection(
                resultSet.all()
                         .stream()
                         .map(row -> new TableName(row.getString("keyspace_name"), row.getString("table_name")))
                         .collect(Collectors.toList()));
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        LOGGER.info("doGetTable: enter", request.getTableName());
        try (CqlSession cassandraCqlSession = cassandraSessionFactory.getSession()) {
            Schema schema = getSchema(cassandraCqlSession, request.getTableName(), null);
            GetTableResponse getTableResponse = new GetTableResponse(null, request.getTableName(), schema, null);
            return getTableResponse;
        }
        catch (Exception exception) {
            throw new RuntimeException();  // todo
        }
    }

    /**
     *
     * @param cqlSession
     * @param tableName
     * @param partitionSchema
     * @return
     */
    Schema getSchema(CqlSession cqlSession, TableName tableName, Schema partitionSchema)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.columns WHERE " +
                                                         "keyspace_name = '" + tableName.getSchemaName() + "' and table_name = '" + tableName.getTableName() + "';");

        //SELECT * FROM system_schema.columns WHERE keyspace_name = 'nytaxi' and table_name = 'fares' and kind in ('partition_key','clustering');"

        resultSet.all().stream().forEach(row -> {
            String cassandraType = row.getString("type");
            ArrowType columnType = CassandraToArrowUtils.getArrowTypeForCassandraField(
                    new CassandraFieldInfo(row.getString("type")), null);
            String columnName = row.getString("column_name");
            String clusteringOrder = row.getString("clustering_order");
            String kind = row.getString("kind");
            int position = row.getInt("position");

            if (columnType != null && SupportedTypes.isSupported(columnType)) {
                schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
            } else {
                LOGGER.error(
                        "getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
            }
        });

        // add partition columns
        partitionSchema.getFields().forEach(schemaBuilder::addField);

        return schemaBuilder.build();
    }

    /**
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param getTableLayoutRequest
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker)
    {

        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(),
                    getTableLayoutRequest.getTableName().getSchemaName(),
                    getTableLayoutRequest.getTableName().getTableName());

        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            // get partition_keys
            ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.columns WHERE keyspace_name = '"
                                                             + getTableLayoutRequest.getTableName().getSchemaName()
                                                             + "' and table_name = '"
                                                             + getTableLayoutRequest.getTableName().getTableName() + "'" +
                                                             "' and kind = 'partition_key'" +
                                                             ";");

            Iterator<Row> rows = resultSet.iterator();

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

            // get clustering_keys?
            ResultSet clusteringKeysResultSet = cqlSession.execute(
                    "SELECT * FROM system_schema.columns WHERE keyspace_name = '"
                            + getTableLayoutRequest.getTableName().getSchemaName()
                            + "' and table_name = '"
                            + getTableLayoutRequest.getTableName().getTableName() + "'" +
                            "' and kind = 'partition_key'" +
                            ";");

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
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws Exception
    {

        return null;
    }

    protected CassandraCredentialProvider getCredentialProvider()
    {
        return new StaticCassandraCredentialProvider();
    }

    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                                                   .addField(BLOCK_PARTITION_SCHEMA_COLUMN_NAME,
                                                             Types.MinorType.VARCHAR.getType())
                                                   .addField(BLOCK_PARTITION_COLUMN_NAME,
                                                             Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }
}
