package com.amazonaws.connectors.athena.cassandra;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraCredentialProvider;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSessionFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SyncCqlSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * @see JdbcMetadataHandlerTest.java
 */

public class CassandraMetadataHandlerTest {

    CqlSession cqlSession;
    private CassandraSessionConfig cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
    private CassandraMetadataHandler cassandraMetadataHandler;
    private CassandraSessionFactory cassandraSessionFactory;
    private CqlSession session;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private BlockAllocator blockAllocator;

    @Before
    public void setup()
    {
        this.cassandraSessionFactory = Mockito.mock(CassandraSessionFactory.class);
        this.session = Mockito.mock(CqlSession.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.cassandraSessionFactory.getSession(Mockito.any(CassandraCredentialProvider.class))).thenReturn(this.session);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        this.cassandraMetadataHandler = new CassandraMetadataHandler(cassandraSessionConfig, this.cassandraSessionFactory, this.secretsManager, this.athena);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
        cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());
        this.blockAllocator = Mockito.mock(BlockAllocator.class);
    }

    /**
     * Since v 6.0 Docs
     *
     * Get keyspaces info
     *
     * SELECT * FROM system_schema.keyspaces
     *
     * Get tables info
     *
     * SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name';
     *
     * Get table info
     *
     * SELECT * FROM system_schema.columns
     * WHERE keyspace_name = 'keyspace_name' AND table_name = 'table_name';
     */

    // TESTS WITH CASSANDRA ON LOCALHOST
    // https://docs.datastax.com/en/pdf/osscql3x.pdf
    @Test
    public void doListTables() throws Exception {
        //ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name'");
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.keyspaces");

       /* resultSet = cqlSession.execute("SELECT * FROM system_schema.tables");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));


        resultSet = cqlSession.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'simplex';");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));*/

        resultSet = cqlSession.execute("SELECT * FROM system_schema.columns WHERE keyspace_name = 'simplex' and table_name = 'songs';");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));
        //System.out.println(resultSet.getColumnDefinitions());

        ListTablesResponse listTablesResponse = this.cassandraMetadataHandler.doListTables(
                this.blockAllocator, new ListTablesRequest(this.federatedIdentity, "testQueryId", "testCatalog", "testSchema"));


    }

    @Test
    public void doGetSchema(){

    }

}
