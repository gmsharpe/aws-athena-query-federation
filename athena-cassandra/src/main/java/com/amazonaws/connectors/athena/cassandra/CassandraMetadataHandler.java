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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CassandraMetadataHandler extends MetadataHandler {

    private final CassandraSessionFactory cassandraSessionFactory;
    private final CassandraSessionConfig cassandraSessionConfig;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataHandler.class);

    private static final String SOURCE_TYPE = "cassandra";

    protected CassandraMetadataHandler(final CassandraSessionConfig cassandraSessionConfig, final CassandraSessionFactory cassandraSessionFactory) {
        super(SOURCE_TYPE);
        this.cassandraSessionFactory = Validate.notNull(cassandraSessionFactory, "cassandraSessionFactory must not be null");
        this.cassandraSessionConfig = Validate.notNull(cassandraSessionConfig, "cassandraSessionConfig must not be null");
    }


    public CassandraMetadataHandler(EncryptionKeyFactory encryptionKeyFactory,
                                    AWSSecretsManager secretsManager,
                                    AmazonAthena athena,
                                    String spillBucket,
                                    String spillPrefix) {
        super(encryptionKeyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);

        cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
        cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();

    }

    public CassandraMetadataHandler() {
        super(SOURCE_TYPE);
        cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
        cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();
    }

    public CassandraMetadataHandler(CassandraSessionConfig cassandraSessionConfig,
                                    CassandraSessionFactory cassandraSessionFactory,
                                    AWSSecretsManager secretsManager,
                                    AmazonAthena athena) {

        // todo can/should null values be passed here?
        super(null,secretsManager,athena,SOURCE_TYPE,null,null);
        this.cassandraSessionFactory = Validate.notNull(cassandraSessionFactory, "cassandraSessionFactory must not be null");
        this.cassandraSessionConfig = Validate.notNull(cassandraSessionConfig, "cassandraSessionConfig must not be null");
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) throws Exception {


        return null;
    }

    /**
     * https://stackoverflow.com/questions/38696316/how-to-list-all-cassandra-tables
     * https://docs.datastax.com/en/pdf/osscql3x.pdf
     *
     * @param allocator         Tool for creating and managing Apache Arrow Blocks.
     * @param listTablesRequest Provides details on who made the request and which Athena catalog and database they are querying.
     * @return
     * @throws Exception
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest listTablesRequest) throws Exception {

        CqlSession cqlSession = cassandraSessionFactory.getSession(getCredentialProvider());
        LOGGER.info("{}: List table names for Catalog {}, Table {}", listTablesRequest.getQueryId(), listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
        return new ListTablesResponse(listTablesRequest.getCatalogName(), listTables(cqlSession, listTablesRequest.getSchemaName()));
    }

    private Collection<TableName> listTables(CqlSession cqlSession, String schemaName) {
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables");

        return Collections.unmodifiableCollection(
                resultSet.all()
                        .stream()
                        .map(row -> new TableName(row.getString("keyspace_name"), row.getString("table_name")))
                                .collect(Collectors.toList()));

    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) throws Exception {

        LOGGER.info("doGetTable: enter", request.getTableName());
        Schema origSchema = null;

/*        if (origSchema == null) {
            origSchema = HbaseSchemaUtils.inferSchema(getOrCreateConn(request), request.getTableName(), NUM_ROWS_TO_SCAN);
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        origSchema.getFields().forEach((Field field) ->
                schemaBuilder.addField(field.getName(), field.getType(), field.getChildren())
        );

        origSchema.getCustomMetadata().entrySet().forEach((Map.Entry<String, String> meta) ->
                schemaBuilder.addMetadata(meta.getKey(), meta.getValue()));

        schemaBuilder.addField(HbaseSchemaUtils.ROW_COLUMN_NAME, Types.MinorType.VARCHAR.getType());*/

       /* Schema schema = schemaBuilder.build();
        LOGGER.info("doGetTable: return {}", schema);
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);*/

        return null;
    }

    private Schema getSchema(CqlSession cqlSession, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.columns WHERE " +
                "keyspace_name = '" + tableName.getSchemaName() + "' and table_name = '" + tableName.getTableName() +"';");

        resultSet.all().stream().forEach( row -> {
                // todo should I also retrieve 'column_name_bytes' from column schema
                ArrowType columnType = CassandraToArrowTypeConverter.toArrowType(
                        row.getString("type"),
                        row.getInt("position"),
                        row.getString("clustering_order"));
                String columnName = row.getString("column_name");
                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());

                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            });

        // add partition columns
        partitionSchema.getFields().forEach(schemaBuilder::addField);

        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {

    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) throws Exception {

        return null;

    }

    protected CassandraCredentialProvider getCredentialProvider() {
        /*final String secretName = cassandraSessionConfig.getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            LOGGER.info("Using Secrets Manager.");
            return new RdsSecretsCredentialProvider(getSecret(secretName));
        }

        return null;*/
        return new StaticCassandraCredentialProvider();
    }

}
