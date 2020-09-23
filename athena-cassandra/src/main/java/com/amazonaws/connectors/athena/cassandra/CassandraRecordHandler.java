package com.amazonaws.connectors.athena.cassandra;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionFactory;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.util.Map;

public class CassandraRecordHandler
        extends RecordHandler
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraRecordHandler.class);
    private static final String sourceType = "cassandra";

    //private final CqlSessionCredentialProvider cqlSessionCredentialProvider;
    private final CassandraSessionFactory cassandraSessionFactory;
    private final CassandraSessionConfig cassandraSessionConfig;

    private final CassandraSplitQueryBuilder cassandraSplitQueryBuilder;

    public static final org.joda.time.MutableDateTime EPOCH = new org.joda.time.MutableDateTime();

    public CassandraRecordHandler()
    {
        super(sourceType);
        /** todo: how to input connection config?
         * Initiates a connection to the session specified by the application.conf.
         */
        cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();
        cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
        cassandraSplitQueryBuilder = new CassandraSplitQueryBuilder();
    }

    @VisibleForTesting
    public CassandraRecordHandler(final CqlSession cassandraCqlSession,
                                  final AmazonS3 amazonS3,
                                  final AWSSecretsManager secretsManager,
                                  final AmazonAthena athena,
                                  final String sourceType,
                                  final CassandraSessionFactory cassandraSessionFactory,
                                  final CassandraSessionConfig cassandraSessionConfig)
    {

        super(amazonS3, secretsManager, athena, sourceType);
        this.cassandraSessionFactory = cassandraSessionFactory;
        this.cassandraSessionConfig = cassandraSessionConfig;
        cassandraSplitQueryBuilder = new CassandraSplitQueryBuilder();
    }

    @VisibleForTesting
    public CassandraRecordHandler(CqlSession cqlSession,
                                  AmazonS3 amazonS3,
                                  AWSSecretsManager secretsManager,
                                  AmazonAthena athena)
    {
        this(cqlSession,
             amazonS3,
             secretsManager,
             athena,
             sourceType,
             CassandraSessionFactory.getDefaultSessionFactory(),
             CassandraSessionConfig.getDefaultSessionConfig());
    }

    // todo do I need a ThrottlingInvoker.ExceptionFilter?

    @Override
    protected void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest,
                                      QueryStatusChecker queryStatusChecker) throws Exception
    {

        logger.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(),
                    readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSplit().getProperties());
        try (CqlSession cassandraCqlSession = cassandraSessionFactory.getSession()) {

            try {
                Statement statement = buildSplitSql(cassandraCqlSession, readRecordsRequest.getCatalogName(),
                                                    readRecordsRequest.getTableName(),
                                                    readRecordsRequest.getSchema(),
                                                    readRecordsRequest.getConstraints(),
                                                    readRecordsRequest.getSplit());

                ResultSet resultSet = cassandraCqlSession.execute(statement);

                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(
                        readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    // use first row in resultSet to make extractors
                    Extractor extractor = makeExtractor(next, resultSet.one(), partitionValues);
                    rowWriterBuilder.withExtractor(next.getName(), extractor);
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;

                for (Row row : resultSet) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows(
                            (Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, row) ? 1 : 0);
                    rowsReturnedFromDatabase++;
                }
                logger.info("{} rows returned by database.", rowsReturnedFromDatabase);
            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    /**
     * Borrowed From : <code>JdbcRecordHandler</code>
     * Creates an Extractor for the given field. In this example the extractor just creates some random data.
     */
    private Extractor makeExtractor(Field field, Row row, Map<String, String> partitionValues)
    {

        // why get the MinorType?
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        final String fieldName = field.getName();

        if (partitionValues.containsKey(fieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(fieldName);
            };
        }

        /**
         *
         * This currently maps Arrow -> Cassandra, however, it is used to extract an Arrow value from the Cassandra Type, and there
         * are not one to one mappings.  Example:  MinorType.VARCHAR -> could be used to map to UUID, Text, VARCHAR, but should
         * it be used this way?
         *
         * for LocalDate and LocalTime conversions:  https://www.concretepage.com/java/java-8/convert-between-java-localdate-epoch
         *
         * NOTE:  TODO there are more Cassandra Data Types (and associated Arrow Minor Types) that could be extracted here.
         *
         */

        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    boolean value = row.getBoolean(fieldName);
                    dst.value = value ? 1 : 0;
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    dst.value = row.getByte(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    dst.value = row.getShort(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) ->
                {
                    dst.value = row.getInt(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    dst.value = row.getLong(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    dst.value = row.getFloat(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    dst.value = row.getDouble(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder dst) ->
                {
                    dst.value = row.getBigDecimal(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    if (row.getLocalDate(fieldName) != null) {
                        dst.value = Long.valueOf(row.getLocalDate(fieldName).toEpochDay()).intValue();
                    }
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    if (row.getLocalDate(fieldName) != null) {
                        // TODO review.
                        dst.value = row.getLocalDate(fieldName).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
                    }
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    dst.value = row.getString(fieldName);
                    dst.isSet = row.isNull(fieldName) ? 0 : 1;
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    ColumnDefinition colDef = row.getColumnDefinitions().get(fieldName);
                    if (colDef.getType().getProtocolCode() == DataTypes.UUID.getProtocolCode()) {
                        dst.value = row.getUuid(fieldName).toString().getBytes();
                        dst.isSet = row.isNull(fieldName) ? 0 : 1;
                    } else {
                        dst.value = row.getByteBuffer(fieldName).array();
                        dst.isSet = row.isNull(fieldName) ? 0 : 1;
                    }
                };
/*            case FIXEDSIZEBINARY:
                return (FixedSizeBinaryExtractor)(Object context, NullableFixedSizeBinaryHolder dst) -> {
                    ColumnDefinition colDef = row.getColumnDefinitions().get(fieldName);
                    if(colDef.getType().getProtocolCode() ==  DataTypes.UUID.getProtocolCode()){
                        dst.byteWidth = 16;
                        dst.value = row.getUuid(fieldName).toString().getBytes();
                        dst.isSet = row.isNull(fieldName) ? 0 : 1;
                    }
            };*/
            default:
                throw new RuntimeException("Unhandled type " + fieldType);
        }
    }

    public Statement buildSplitSql(CqlSession cqlSession, String catalogName, TableName tableName, Schema schema,
                                   Constraints constraints, Split split)
    {
        Statement statement = cassandraSplitQueryBuilder.buildSql(cqlSession, null, tableName.getSchemaName(),
                                                                  tableName.getTableName(), schema, constraints, split);
        // todo - what should the pageSize be?
        statement.setPageSize(1000);
        return statement;
    }
}
