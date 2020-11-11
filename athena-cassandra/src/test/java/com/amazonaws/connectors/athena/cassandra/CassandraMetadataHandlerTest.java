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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.datastax.oss.driver.api.core.session.Session;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.sql.SQLException;


/**
 * From <code>JdbcMetadataHandlerTest</code>
 */

public class  CassandraMetadataHandlerTest {

    private CassandraSessionConfig cassandraSessionConfig = CassandraSessionConfig.getDefaultSessionConfig();
    private CassandraMetadataHandler cassandraMetadataHandler;
    private CassandraSessionFactory cassandraSessionFactory;
    private CqlSession cqlSession;
    private FederatedIdentity federatedIdentity;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private BlockAllocator blockAllocator;

    @Before
    public void setup()
    {
        this.cassandraSessionFactory = Mockito.mock(CassandraSessionFactory.class);
        //this.session = Mockito.mock(CqlSession.class, Mockito.RETURNS_DEEP_STUBS);
        this.cqlSession = CqlSession.builder().build();//.addContactPoint(InetSocketAddress.createUnresolved("127.0.0.1",9042)).build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());

        Mockito.when(this.cassandraSessionFactory.getSession(Mockito.any(CassandraCredentialProvider.class))).thenReturn(this.cqlSession);
        secretsManager = Mockito.mock(AWSSecretsManager.class);

        Mockito.when(secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret"))))
               .thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        cassandraMetadataHandler = new CassandraMetadataHandler(cassandraSessionConfig,
                                                                cassandraSessionFactory,
                                                                secretsManager,
                                                                athena);
        federatedIdentity = Mockito.mock(FederatedIdentity.class);


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
    public void doGetSchema() throws SQLException {

        Schema schema = cassandraMetadataHandler
                .getSchema(cqlSession,new TableName("nytaxi","fares"), SchemaBuilder.newBuilder().build());

        System.out.println(schema.toJson());

    }

}
