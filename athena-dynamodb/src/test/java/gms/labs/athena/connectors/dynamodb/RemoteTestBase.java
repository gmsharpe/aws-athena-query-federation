/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package gms.labs.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.throttling.DynamoDBExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toAttributeValue;
import static com.amazonaws.services.dynamodbv2.document.ItemUtils.toItem;
import static com.amazonaws.services.dynamodbv2.model.BillingMode.*;
import static com.amazonaws.services.dynamodbv2.model.KeyType.*;
import static com.amazonaws.services.dynamodbv2.model.ScalarAttributeType.*;
import static gms.labs.athena.connectors.dynamodb.RemoteTestBase.ddbCols.*;

public class RemoteTestBase
{

    /*
        re-writing the TestBase to use the sample tables provided in DynamoDb Tutorial:
         * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CodeSamples.html

     */

    protected FederatedIdentity TEST_IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());

    protected static final String TEST_QUERY_ID = "queryId";
    protected static final String TEST_CATALOG_NAME = "default";

    protected static final String TEST_TABLE_1 = "test_table_1";
    protected static final String FORUM = "Forum";
    protected static final String REPLY = "Reply";
    protected static final String THREAD = "Thread";

    protected static final TableName PRODUCT_CATALOG_TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE_1);
    protected static final TableName FORUM_TABLE_NAME = new TableName(DEFAULT_SCHEMA, FORUM);
    protected static final TableName REPLY_TABLE_NAME = new TableName(DEFAULT_SCHEMA, REPLY);
    protected static final TableName THREAD_TABLE_NAME = new TableName(DEFAULT_SCHEMA, THREAD);

    protected static AmazonDynamoDB ddbClient;
    protected static Schema schema;
    protected static Table tableDdbNoGlue;

    @BeforeClass
    public static void setupOnce() throws Exception
    {
        ddbClient = setupRemoteDatabase();
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
        schema = DDBTableUtils.peekTableForSchema(TEST_TABLE_1, invoker, ddbClient);
    }

    @AfterClass
    public static void tearDownOnce()
    {
        ddbClient.deleteTable(new DeleteTableRequest().withTableName(TEST_TABLE_1));
        ddbClient.deleteTable(new DeleteTableRequest().withTableName(FORUM));
        ddbClient.deleteTable(new DeleteTableRequest().withTableName(REPLY));
        ddbClient.deleteTable(new DeleteTableRequest().withTableName(THREAD));
        ddbClient.shutdown();
    }
    
    // cols
    enum ddbCols {
        STRING,
        INT,
        DOUBLE,
        EPOCH_DAY_LONG,
        EPOCH_MILLI_LONG
    }

    private static AmazonDynamoDB setupRemoteDatabase() throws InterruptedException
    {
        ddbClient = AmazonDynamoDBClient.builder()
                                       .withCredentials(new DefaultAWSCredentialsProviderChain())
                                       .withRegion(Regions.US_WEST_1)
                                       .build();

        DynamoDB ddb = new DynamoDB(ddbClient);

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("col_0").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("col_1").withAttributeType("N"));

        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("col_0").withKeyType(HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("col_1").withKeyType(RANGE));

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withTableName(TEST_TABLE_1)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withBillingMode(PAY_PER_REQUEST);


        Table table = ddb.createTable(createTableRequest);

        table.waitForActive();

        TableWriteItems tableWriteItems = new TableWriteItems(TEST_TABLE_1);
        int len = 1000;
        LocalDateTime dateTime = LocalDateTime.of(2019, 9, 23, 11, 18, 37);
        for (int i = 0; i < len; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            
            // string
            item.put(STRING.name(), toAttributeValue("test_str_" + (i - i % 3)));
            
            // int
            item.put(INT.name(), toAttributeValue(i));
            
            // double
            double doubleVal = 200000.0 + i / 2.0;
            if (Math.floor(doubleVal) != doubleVal) {
                item.put(DOUBLE.name(), toAttributeValue(200000.0 + i / 2.0));
            }
            
            // complex object (map)
            item.put("col_3", toAttributeValue(ImmutableMap.of("modulo", i % 2, "nextModulos", ImmutableList.of((i + 1) % 2, ((i + 2) % 2)))));
            
            // long (epoch day)
            item.put(EPOCH_DAY_LONG.name(), toAttributeValue(dateTime.toLocalDate().toEpochDay()));
            
            // long (epic milli)
            item.put(EPOCH_MILLI_LONG.name(), toAttributeValue(Timestamp.valueOf(dateTime).toInstant().toEpochMilli()));
            
            // mix of int and null values
            item.put("col_6", toAttributeValue(i % 128 == 0 ? null : i % 128));
            
            // list of strings
            item.put("col_7", toAttributeValue(ImmutableList.of(-i, String.valueOf(i))));
            
            // mostly empty map
            item.put("col_8", toAttributeValue(ImmutableList.of(ImmutableMap.of("mostlyEmptyMap",
                    i % 128 == 0 ? ImmutableMap.of("subtractions", ImmutableSet.of(i - 100, i - 200)) : ImmutableMap.of()))));
            
            // float
            item.put("col_9", toAttributeValue(100.0f + i));
            
            // lists of lists of numbers
            item.put("col_10", toAttributeValue(ImmutableList.of(ImmutableList.of(1 * i, 2 * i, 3 * i),
                    ImmutableList.of(4 * i, 5 * i), ImmutableList.of(6 * i, 7 * i, 8 * i))));
            
            
            
            tableWriteItems.addItemToPut(toItem(item));

            if (tableWriteItems.getItemsToPut().size() == 25) {
                ddb.batchWriteItem(tableWriteItems);
                tableWriteItems = new TableWriteItems(TEST_TABLE_1);
            }

            dateTime = dateTime.plusHours(26);
        }

        CreateGlobalSecondaryIndexAction createIndexRequest = new CreateGlobalSecondaryIndexAction()
                .withIndexName("test_index")
                .withKeySchema(
                        new KeySchemaElement().withKeyType(HASH).withAttributeName(EPOCH_DAY_LONG.name()),
                        new KeySchemaElement().withKeyType(RANGE).withAttributeName(EPOCH_MILLI_LONG.name()))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL));

        Index gsi = table.createGSI(createIndexRequest,
                new AttributeDefinition().withAttributeName(EPOCH_DAY_LONG.name()).withAttributeType(N),
                new AttributeDefinition().withAttributeName(EPOCH_MILLI_LONG.name()).withAttributeType(N));
        gsi.waitForActive();

        // for case sensitivity testing
        createTableRequest = new CreateTableRequest()
                .withTableName(FORUM)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withBillingMode(PAY_PER_REQUEST);
        table = ddb.createTable(createTableRequest);
        table.waitForActive();

        ArrayList<AttributeDefinition> attributeDefinitionsGlue = new ArrayList<>();
        attributeDefinitionsGlue.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));
        attributeDefinitionsGlue.add(new AttributeDefinition().withAttributeName("Col1").withAttributeType("S"));

        ArrayList<KeySchemaElement> keySchemaGlue = new ArrayList<>();
        keySchemaGlue.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(HASH));
        keySchemaGlue.add(new KeySchemaElement().withAttributeName("Col1").withKeyType(RANGE));

        CreateTableRequest createTableRequestGlue = new CreateTableRequest()
                .withTableName(REPLY)
                .withKeySchema(keySchemaGlue)
                .withAttributeDefinitions(attributeDefinitionsGlue)
                .withBillingMode(PAY_PER_REQUEST);

        Table tableGlue = ddb.createTable(createTableRequestGlue);
        tableGlue.waitForActive();

        tableWriteItems = new TableWriteItems(REPLY);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("Col1", toAttributeValue("20200227S091227"));
        item.put("Col2", toAttributeValue("2020-02-27T09:12:27Z"));
        item.put("Col3", toAttributeValue("27/02/2020"));
        item.put("Col4", toAttributeValue("2020-02-27"));
        // below three columns are testing timestamp with timezone
        // col5 with non-utc timezone, col6 with utc timezone, and c7 without timezone that will fall back to   default
        item.put("Col5", toAttributeValue("2015-12-21T17:42:34-05:00"));
        item.put("Col6", toAttributeValue("2015-12-21T17:42:34Z"));
        item.put("Col7", toAttributeValue("2015-12-21T17:42:34"));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);

        // Table 4
        attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Col0").withAttributeType("S"));

        keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Col0").withKeyType(HASH));

        createTableRequest = new CreateTableRequest()
                .withTableName(THREAD)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withBillingMode(PAY_PER_REQUEST);

        tableDdbNoGlue = ddb.createTable(createTableRequest);
        tableDdbNoGlue.waitForActive();

        Map<String, String> col1 = new HashMap<>();
        col1.put("field1", "someField1");
        col1.put("field2", null);

        tableWriteItems = new TableWriteItems(THREAD);
        item = new HashMap<>();
        item.put("Col0", toAttributeValue("hashVal"));
        item.put("Col1", toAttributeValue(col1));
        tableWriteItems.addItemToPut(toItem(item));
        ddb.batchWriteItem(tableWriteItems);

        return ddbClient;
    }
}
