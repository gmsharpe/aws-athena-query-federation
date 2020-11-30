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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler;
import com.amazonaws.athena.connectors.dynamodb.TestBase;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.joda.time.MutableDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.*;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.DYNAMO_DB_FLAG;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.MAX_SPLITS_PER_REQUEST;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Glue logic is tested by GlueMetadataHandlerTest in SDK
 */
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBMetadataHandlerTest
        extends gms.labs.athena.connectors.dynamodb.TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandlerTest.class);

    @Rule
    public TestName testName = new TestName();

    @Mock
    private AWSGlue glueClient;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    private DynamoDBMetadataHandler dynamoDbMetadataHandler;

    private BlockAllocator allocator;

    AmazonDynamoDB dbClient;

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        allocator = new BlockAllocatorImpl();

        dbClient = AmazonDynamoDBClient.builder()
                                       .withCredentials(new DefaultAWSCredentialsProviderChain())
                                       .withRegion(Regions.US_WEST_1)
                                       .build();

        dynamoDbMetadataHandler = new DynamoDBMetadataHandler(new LocalKeyFactory(),
                                                              secretsManager,
                                                              athena,
                                                              "spillBucket",
                                                              "spillPrefix",
                                                              dbClient,
                                                              glueClient);

    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNamesGlueError()
            throws Exception
    {
        when(glueClient.getDatabases(any())).thenThrow(new AmazonServiceException(""));

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = dynamoDbMetadataHandler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(new ArrayList<>(res.getSchemas()), equalTo(Collections.singletonList(DEFAULT_SCHEMA)));
    }

    @Test
    public void doListSchemaNamesGlue()
            throws Exception
    {
        GetDatabasesResult result = new GetDatabasesResult().withDatabaseList(
                new Database().withName(DEFAULT_SCHEMA),
                new Database().withName("ddb").withLocationUri(DYNAMO_DB_FLAG),
                new Database().withName("s3").withLocationUri("blah"));

        when(glueClient.getDatabases(any())).thenReturn(result);

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = dynamoDbMetadataHandler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(res.getSchemas().size(), equalTo(2));
        assertThat(res.getSchemas().contains("default"), is(true));
        assertThat(res.getSchemas().contains("ddb"), is(true));
    }

    @Test
    public void doListTablesGlueAndDynamo()
            throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        GetTablesResult mockResult = new GetTablesResult();
        List<Table> tableList = new ArrayList<>();
        tableList.add(new Table().withName("table1")
                .withParameters(ImmutableMap.of("classification", "dynamodb"))
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")));
        tableList.add(new Table().withName("table2")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("some.location")
                        .withParameters(ImmutableMap.of("classification", "dynamodb"))));
        tableList.add(new Table().withName("table3")
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation("arn:aws:dynamodb:us-east-1:012345678910:table/table3")));
        tableList.add(new Table().withName("notADynamoTable").withParameters(ImmutableMap.of()).withStorageDescriptor(
                new StorageDescriptor().withParameters(ImmutableMap.of()).withLocation("some_location")));
        mockResult.setTableList(tableList);
        when(glueClient.getTables(any())).thenReturn(mockResult);

        ListTablesRequest req = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, DEFAULT_SCHEMA);
        ListTablesResponse res = dynamoDbMetadataHandler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        List<TableName> expectedTables = tableNames.stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(Collectors.toList());
        expectedTables.add(PRODUCT_CATALOG_TABLE_NAME);
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table2"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table3"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table4"));

        assertThat(new HashSet<>(res.getTables()), equalTo(new HashSet<>(expectedTables)));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, PRODUCT_CATALOG_TABLE_NAME);
        GetTableResponse res = dynamoDbMetadataHandler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName().getSchemaName(), equalTo(DEFAULT_SCHEMA));
        assertThat(res.getTableName().getTableName(), equalTo(PRODUCT_CATALOG));
        //assertThat(res.getSchema().getFields().size(), equalTo(11));
    }

    @Test
    public void doGetEmptyTable()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, FORUM_TABLE_NAME);
        GetTableResponse res = dynamoDbMetadataHandler.doGetTable(allocator, req);

        logger.info("doGetEmptyTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(FORUM_TABLE_NAME));
        assertThat(res.getSchema().getFields().size(), equalTo(2));
    }

    @Test
    public void testCaseInsensitiveResolve()
            throws Exception
    {
        when(glueClient.getTable(any())).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, FORUM_TABLE_NAME);
        GetTableResponse res = dynamoDbMetadataHandler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(FORUM_TABLE_NAME));
    }

    @Test
    public void doGetTableLayoutScan()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_CATALOG_NAME, PRODUCT_CATALOG),
                new Constraints(constraintsMap),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = dynamoDbMetadataHandler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_3 = :v0 OR attribute_not_exists(#col_3) OR #col_3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_3", "col_3");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }

    @Test
    public void doGetTableLayoutQueryIndex()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        SortedRangeSet.Builder dateValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEDAY.getType(), false);
        SortedRangeSet.Builder timeValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
        LocalDateTime dateTime = new LocalDateTime().withYear(2019).withMonthOfYear(9).withDayOfMonth(23).withHourOfDay(11).withMinuteOfHour(18).withSecondOfMinute(37);
        MutableDateTime epoch = new MutableDateTime();
        epoch.setDate(0); //Set to Epoch time
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), Days.daysBetween(epoch, dateTime.toDateTime()).getDays()));
        LocalDateTime dateTime2 = dateTime.plusHours(26);
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), Days.daysBetween(epoch, dateTime2.toDateTime()).getDays()));
        long startTime = dateTime.toDateTime().getMillis();
        long endTime = dateTime2.toDateTime().getMillis();
        timeValueSet.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime, true,
                endTime, true));
        constraintsMap.put("col_4", dateValueSet.build());
        constraintsMap.put("col_5", timeValueSet.build());

        GetTableLayoutResponse res = dynamoDbMetadataHandler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                                                                                                                   TEST_QUERY_ID,
                                                                                                                   TEST_CATALOG_NAME,
                                                                                                                   PRODUCT_CATALOG_TABLE_NAME,
                                                                                                                   new Constraints(constraintsMap),
                                                                                                                   SchemaBuilder.newBuilder().build(),
                                                                                                                   Collections.EMPTY_SET));

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(QUERY_PARTITION_TYPE));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().containsKey(INDEX_METADATA), is(true));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(INDEX_METADATA), equalTo("test_index"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(HASH_KEY_NAME_METADATA), equalTo("col_4"));
        assertThat(res.getPartitions().getRowCount(), equalTo(2));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_NAME_METADATA), equalTo("col_5"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 >= :v0 AND #col_5 <= :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_4", "col_4", "#col_5", "col_5");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(startTime), ":v1", ItemUtils.toAttributeValue(endTime));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }

    @Test
    public void doGetSplitsScan()
            throws Exception
    {
        GetTableLayoutResponse layoutResponse = dynamoDbMetadataHandler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                                                                                                                              TEST_QUERY_ID,
                                                                                                                              TEST_CATALOG_NAME,
                                                                                                                              PRODUCT_CATALOG_TABLE_NAME,
                                                                                                                              new Constraints(ImmutableMap.of()),
                                                                                                                              SchemaBuilder.newBuilder().build(),
                                                                                                                              Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                PRODUCT_CATALOG_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of(),
                new Constraints(new HashMap<>()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = dynamoDbMetadataHandler.doGetSplits(allocator, req);
        assertThat(rawResponse.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken == null, is(true));

        Split split = Iterables.getOnlyElement(response.getSplits());
        assertThat(split.getProperty(SEGMENT_ID_PROPERTY), equalTo("0"));

        logger.info("doGetSplitsScan: exit");
    }

    @Test
    public void doGetSplitsQuery()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        EquatableValueSet.Builder valueSet = EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false);
        for (int i = 0; i < 2000; i++) {
            valueSet.add("test_str_" + i);
        }
        constraintsMap.put("col_0", valueSet.build());
        GetTableLayoutResponse layoutResponse = dynamoDbMetadataHandler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                                                                                                                              TEST_QUERY_ID,
                                                                                                                              TEST_CATALOG_NAME,
                                                                                                                              PRODUCT_CATALOG_TABLE_NAME,
                                                                                                                              new Constraints(constraintsMap),
                                                                                                                              SchemaBuilder.newBuilder().build(),
                                                                                                                              Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                PRODUCT_CATALOG_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of("col_0"),
                new Constraints(new HashMap<>()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = dynamoDbMetadataHandler.doGetSplits(allocator, req);
        assertThat(response.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken, equalTo(String.valueOf(MAX_SPLITS_PER_REQUEST - 1)));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));

        response = dynamoDbMetadataHandler.doGetSplits(allocator, new GetSplitsRequest(req, continuationToken));

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(response.getContinuationToken(), equalTo(null));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));
    }

    @Test
    public void validateSourceTableNamePropagation()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int"));
        columns.add(new Column().withName("col2").withType("bigint"));
        columns.add(new Column().withName("col3").withType("string"));

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, PRODUCT_CATALOG,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1 , col2=Col2 ,col3=Col3",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = dynamoDbMetadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(PRODUCT_CATALOG));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,Col3=datetime3"));

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(ImmutableMap.of()),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse getTableLayoutResponse = dynamoDbMetadataHandler.doGetTableLayout(allocator, getTableLayoutRequest);
        logger.info("validateSourceTableNamePropagation: GetTableLayoutResponse[{}]", getTableLayoutResponse);
        assertThat(getTableLayoutResponse.getPartitions().getSchema().getCustomMetadata().get(TABLE_METADATA), equalTo(PRODUCT_CATALOG));
    }

    @Test
    public void doGetTableLayoutScanWithTypeOverride()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int"));
        columns.add(new Column().withName("col2").withType("timestamptz"));
        columns.add(new Column().withName("col3").withType("string"));

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, PRODUCT_CATALOG,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = new Table()
                .withParameters(param)
                .withPartitionKeys()
                .withStorageDescriptor(new StorageDescriptor().withColumns(columns));
        GetTableResult mockResult = new GetTableResult().withTable(table);
        when(glueClient.getTable(any())).thenReturn(mockResult);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName);
        GetTableResponse getTableResponse = dynamoDbMetadataHandler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(PRODUCT_CATALOG));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,col3=datetime3"));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());
        constraintsMap.put("col2",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(constraintsMap),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);


        GetTableLayoutResponse res = dynamoDbMetadataHandler.doGetTableLayout(allocator, getTableLayoutRequest);

        logger.info("doGetTableLayoutScanWithTypeOverride schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayoutScanWithTypeOverride partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col3 = :v0 OR attribute_not_exists(#col3) OR #col3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col3", "col3", "#col2", "col2");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", ItemUtils.toAttributeValue(true), ":v1", ItemUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(Jackson.toJsonString(expressionValues)));
    }
}
