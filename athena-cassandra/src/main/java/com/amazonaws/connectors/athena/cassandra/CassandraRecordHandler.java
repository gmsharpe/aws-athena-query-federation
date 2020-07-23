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
                    


            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            }


        }
    }


    public PreparedStatement buildSplitSql(CqlSession cqlSession, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
    {

        return null;
    }

}
