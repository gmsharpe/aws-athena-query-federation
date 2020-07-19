package com.amazonaws.connectors.athena.cassandra;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraRecordHandler
        extends RecordHandler {

    private static final Logger logger = LoggerFactory.getLogger(CassandraRecordHandler.class);
    private static final String sourceType = "cassandra";

    //private final CqlSessionCredentialProvider cqlSessionCredentialProvider;
    private final CassandraSessionFactory cassandraSessionFactory;
    private final CassandraSessionConfig cassandraSessionConfig;

    public CassandraRecordHandler()
    {
        super(sourceType);
        /** todo: how to input connection config?
         * Initiates a connection to the session specified by the application.conf.
         */
        this.cassandraSessionFactory = CassandraSessionFactory.getDefaultSessionFactory();
        this.cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
    }

    @VisibleForTesting
    public CassandraRecordHandler(final CqlSession cassandraCqlSession,
                                  final AmazonS3 amazonS3,
                                  final AWSSecretsManager secretsManager,
                                  final AmazonAthena athena,
                                  final String sourceType,
                                  final CassandraSessionFactory cassandraSessionFactory,
                                  final CassandraSessionConfig cassandraSessionConfig) {

        super(amazonS3, secretsManager, athena, sourceType);
        this.cassandraSessionFactory = cassandraSessionFactory;
        this.cassandraSessionConfig = cassandraSessionConfig;
    }

    // todo do I need a ThrottlingInvoker.ExceptionFilter?

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker) throws Exception {
        {

            logger.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSplit().getProperties());
            try(CqlSession cassandraCqlSession = this.cassandraSessionFactory.getSession()){

                // connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
                try{


                    PreparedStatement preparedStatement = buildSplitSql(cassandraCqlSession, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                        readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());

                    ResultSet resultSet = cassandraCqlSession.execute((Statement)preparedStatement);

                    Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                 /*   GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                    for (Field next : readRecordsRequest.getSchema().getFields()) {
                        Extractor extractor = makeExtractor(next, resultSet, partitionValues);
                        rowWriterBuilder.withExtractor(next.getName(), extractor);
                    }

                    GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                    int rowsReturnedFromDatabase = 0;
                    while (resultSet.next()) {
                        if (!queryStatusChecker.isQueryRunning()) {
                            return;
                        }
                        blockSpiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, resultSet) ? 1 : 0);
                        rowsReturnedFromDatabase++;
                    }
                    logger.info("{} rows returned by database.", rowsReturnedFromDatabase);*/


            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            }
/*

            // below is DynamoDB readWithConstraint
            Split split = recordsRequest.getSplit();
            // use the property instead of the request table name because of case sensitivity
            String tableName = split.getProperty(TABLE_METADATA);
            //invokerCache.get(tableName).setBlockSpiller(spiller);
            Iterator<Map<String, AttributeValue>> itemIterator = getIterator(split, tableName, recordsRequest.getSchema());
            DDBRecordMetadata recordMetadata = new DDBRecordMetadata(recordsRequest.getSchema());
            long numRows = 0;
            AtomicLong numResultRows = new AtomicLong(0);
            while (itemIterator.hasNext()) {
                if (!queryStatusChecker.isQueryRunning()) {
                    // we can stop processing because the query waiting for this data has already terminated
                    return;
                }
                numRows++;
                spiller.writeRows((Block block, int rowNum) -> {
                    Map<String, AttributeValue> item = itemIterator.next();
                    if (item == null) {
                        // this can happen regardless of the hasNext() check above for the very first iteration since itemIterator
                        // had not made any DDB calls yet and there may be zero items returned when it does
                        return 0;
                    }

                    boolean matched = true;
                    numResultRows.getAndIncrement();
                    // TODO refactor to use GeneratedRowWriter to improve performance
                    for (Field nextField : recordsRequest.getSchema().getFields()) {
                        Object value = ItemUtils.toSimpleValue(item.get(nextField.getName()));
                        Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());

                        value = DDBTypeUtils.coerceValueToExpectedType(value, nextField, fieldType, recordMetadata);

                        try {
                            switch (fieldType) {
                                case LIST:
                                    // DDB may return Set so coerce to List
                                    List valueAsList = value != null ? new ArrayList((Collection) value) : null;
                                    matched &= block.offerComplexValue(nextField.getName(),
                                            rowNum,
                                            FieldResolver.DEFAULT,
                                            valueAsList);
                                    break;
                                case STRUCT:
                                    matched &= block.offerComplexValue(nextField.getName(),
                                            rowNum,
                                            (Field field, Object val) -> ((Map) val).get(field.getName()),
                                            value);
                                    break;
                                default:
                                    matched &= block.offerValue(nextField.getName(), rowNum, value);
                                    break;
                            }

                            if (!matched) {
                                return 0;
                            }
                        } catch (Exception ex) {
                            throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                        }
                    }
                    return 1;
                });
            }
*/

            // logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());

        }
    }

    /*
 Creates an iterator that can iterate through a Query or Scan, sending paginated requests as necessary
  */
   /* private Iterator<Map<String, AttributeValue>> getIterator(Split split, String tableName, Schema schema)
    {
        AmazonWebServiceRequest request = buildReadRequest(split, tableName, schema);
        return new Iterator<Map<String, AttributeValue>>() {
            AtomicReference<Map<String, AttributeValue>> lastKeyEvaluated = new AtomicReference<>();
            AtomicReference<Iterator<Map<String, AttributeValue>>> currentPageIterator = new AtomicReference<>();

            @Override
            public boolean hasNext()
            {
                return currentPageIterator.get() == null
                        || currentPageIterator.get().hasNext()
                        || lastKeyEvaluated.get() != null;
            }

            @Override
            public Map<String, AttributeValue> next()
            {
                if (currentPageIterator.get() != null && currentPageIterator.get().hasNext()) {
                    return currentPageIterator.get().next();
                }
                Iterator<Map<String, AttributeValue>> iterator;
                try {
                    if (request instanceof QueryRequest) {
                        QueryRequest paginatedRequest = ((QueryRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invoking DDB with Query request: {}", request);
                        }
                        QueryResult queryResult = invokerCache.get(tableName).invoke(() -> ddbClient.query(paginatedRequest));
                        lastKeyEvaluated.set(queryResult.getLastEvaluatedKey());
                        iterator = queryResult.getItems().iterator();
                    }
                    else {
                        ScanRequest paginatedRequest = ((ScanRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                        if (logger.isDebugEnabled()) {
                            logger.debug("Invoking DDB with Scan request: {}", request);
                        }
                        ScanResult scanResult = invokerCache.get(tableName).invoke(() -> ddbClient.scan(paginatedRequest));
                        lastKeyEvaluated.set(scanResult.getLastEvaluatedKey());
                        iterator = scanResult.getItems().iterator();
                    }
                }
                catch (TimeoutException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                currentPageIterator.set(iterator);
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                else {
                    return null;
                }
            }
        };
    }
*/


    public PreparedStatement buildSplitSql(CqlSession cqlSession, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        /*PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(cqlSession, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);

        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);

        return preparedStatement;*/
        return null;
    }

}
