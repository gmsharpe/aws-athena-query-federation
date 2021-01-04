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

import com.amazonaws.athena.connector.lambda.data.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.athena.connectors.cassandra.connection.CassandraSessionConfig;
import com.amazonaws.athena.connectors.cassandra.connection.KeyspacesSessionConfig;
import com.amazonaws.athena.connectors.cassandra.connection.KeyspacesSessionFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * From <code>JdbcMetadataHandlerTest</code>
 */

public class CassandraMetadataHandlerTest
{
    private CassandraSessionConfig cassandraSessionConfig;
    private CassandraMetadataHandler cassandraMetadataHandler;
    private KeyspacesSessionFactory cassandraSessionFactory;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private BlockAllocator blockAllocator;

    protected FederatedIdentity TEST_FEDERATED_IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(),
                                                                                Collections.emptyList());
    protected static final String TEST_QUERY_ID = "queryId";
    protected static final String TEST_CATALOG_NAME = "default";
    protected static final String TEST_TABLE = "test_table";

    String keyspace = "elections";
    String tableName = "presidential_election_2016";
    String region = "us-west-1";

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataHandlerTest.class);

    @Before
    public void setup()
    {
        Map<String, String> env = System.getenv();
        AWSCredentials credentials = new BasicAWSCredentials(env.get("AWS_ACCESS_KEY_ID"),
                                                             env.get("AWS_SECRET_ACCESS_KEY"));

        cassandraSessionFactory = new KeyspacesSessionFactory(region);
        cassandraSessionConfig = new KeyspacesSessionConfig(credentials, region);

        secretsManager = Mockito.mock(AWSSecretsManager.class);

        // todo - not used, at the moment.
        Mockito.when(secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret"))))
               .thenReturn(new GetSecretValueResult().withSecretString(
                       "{\"username\": \"testUser\", \"password\": \"testPassword\"}"));

        cassandraMetadataHandler =  new CassandraMetadataHandler(null,
                                                                 secretsManager,
                                                                 athena,
                                                                 null,
                                                                 null,
                                                                 cassandraSessionConfig);
        blockAllocator = new BlockAllocatorImpl();
    }





    /**
     * Since v 6.0 Docs
     * <p>
     * Get keyspaces info
     * <p>
     * SELECT * FROM system_schema.keyspaces
     * <p>
     * Get tables info
     * <p>
     * SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name';
     * <p>
     * Get table info
     * <p>
     * SELECT * FROM system_schema.columns
     * WHERE keyspace_name = 'keyspace_name' AND table_name = 'table_name';
     */

    @Test
    public void doListTables()
    {
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            //ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables");
            //resultSet.forEach(r -> LOGGER.info(r.getFormattedContents()));
            ListTablesResponse listTablesResponse =
                    cassandraMetadataHandler.doListTables(blockAllocator,
                                                          new ListTablesRequest(TEST_FEDERATED_IDENTITY,
                                                                                "testQueryId",
                                                                                keyspace,
                                                                                keyspace));

            listTablesResponse.getTables().stream().map(table -> {
                GetTableResponse response = cassandraMetadataHandler.doGetTable(blockAllocator,
                                                                                new GetTableRequest(
                                                                                        TEST_FEDERATED_IDENTITY,
                                                                                        keyspace,
                                                                                        keyspace,
                                                                                        table));
                return response;
            }).forEach( response -> {
                System.out.println(response.toString());
            });
            
        }
    }

    /*

    @Test
    public void doGetSchema()
    {
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            Schema schema = cassandraMetadataHandler
                    .getTableSchema(cqlSession, new TableName(keyspace, tableName), SchemaBuilder.newBuilder().build());
            LOGGER.info(schema.toJson());
        }
    } */

    @Test
    public void doGetTableSchema()
    {
/*        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            Schema schema = cassandraMetadataHandler
                    .getTableSchema(cqlSession, new TableName(keyspace, tableName), SchemaBuilder.newBuilder().build());
            LOGGER.info(schema.toJson());
        }*/

        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {

            // list all schema (keyspaces)
            ListSchemasResponse listSchemasResponse = cassandraMetadataHandler.doListSchemaNames(blockAllocator,
                                                                                      new ListSchemasRequest(
                                                                                              TEST_FEDERATED_IDENTITY,
                                                                                              "get-schema-names",
                                                                                              keyspace));

            listSchemasResponse.getSchemas().forEach(schemaName -> {

                // list all tables
                ListTablesResponse listTablesResponse =
                        cassandraMetadataHandler.doListTables(blockAllocator,
                                                              new ListTablesRequest(TEST_FEDERATED_IDENTITY,
                                                                                    "testQueryId",
                                                                                    schemaName,
                                                                                    schemaName));

                listTablesResponse.getTables().stream().forEach(table -> {

                    Schema schema = cassandraMetadataHandler.getTableSchema(cqlSession,
                                                                            new TableName(table.getSchemaName(),
                                                                                          table.getTableName()),
                                                                            SchemaBuilder.newBuilder().build());
                    LOGGER.info(schema.toJson());
                });

            });


        }
    }



    @Test
    public void doListTablesFromKeyspaces()
    {
        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables;");

            resultSet.forEach(r -> LOGGER.info(r.getFormattedContents()));

            // todo - should catalog and schema both be Cassandra keyspace
            ListTablesResponse listTablesResponse = cassandraMetadataHandler.doListTables(
                    blockAllocator, new ListTablesRequest(TEST_FEDERATED_IDENTITY, "testQueryId", keyspace, keyspace));

            listTablesResponse.getTables().forEach(table -> LOGGER.info(table.toString()));

            // todo - assertions
        }
    }

    @Test
    public void doGetPartitions()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                           EquatableValueSet.newBuilder(blockAllocator, new ArrowType.Bool(), true, true)
                                            .add(true).build());


        try (CqlSession cqlSession = cassandraSessionFactory.getSession()) {
            GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_FEDERATED_IDENTITY,
                                                                  TEST_QUERY_ID,
                                                                  TEST_CATALOG_NAME,
                                                                  new TableName("elections", tableName),
                                                                  new Constraints(constraintsMap),
                                                                  SchemaBuilder.newBuilder().build(),
                                                                  Collections.EMPTY_SET);

            //Schema schema = cassandraMetadataHandler.getPartitionSchema(keyspace);

            GetTableLayoutResponse layoutResponse = cassandraMetadataHandler.doGetTableLayout(blockAllocator, req);

            Block block = blockAllocator.createBlock(layoutResponse.getPartitions().getSchema());
            BlockWriter blockWriter = new SimpleBlockWriter(block);

            cassandraMetadataHandler.getPartitions(blockWriter,req,null);

            LOGGER.info(layoutResponse.getPartitions().getSchema().toJson());

            // todo - assertions
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // started from DynamoDBMetadataHandlerTest#doGetTableLayoutScan
  /*  @Test
    public void doGetTableLayoutScan()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                           EquatableValueSet.newBuilder(blockAllocator, new ArrowType.Bool(), true, true)
                                            .add(true).build());

        GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_FEDERATED_IDENTITY,
                                                              TEST_QUERY_ID,
                                                              TEST_CATALOG_NAME,
                                                              new TableName(TEST_CATALOG_NAME, TEST_TABLE),
                                                              new Constraints(constraintsMap),
                                                              SchemaBuilder.newBuilder().build(),
                                                              Collections.EMPTY_SET);

        GetTableLayoutResponse res = cassandraMetadataHandler.doGetTableLayout(blockAllocator, req);

        LOGGER.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        LOGGER.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_3 = :v0 OR attribute_not_exists(#col_3) OR #col_3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_3", "col_3");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(
                Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }*/

}
