package gms.labs.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandler;
import com.amazonaws.athena.connectors.dynamodb.DynamoDBRecordHandlerTest;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.*;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DynamoDBRecordHandlerRIT
{

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandlerRIT.class);

    protected FederatedIdentity TEST_IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());

    private static final SpillLocation SPILL_LOCATION = S3SpillLocation.newBuilder()
                                                                       .withBucket(UUID.randomUUID().toString())
                                                                       .withSplitId(UUID.randomUUID().toString())
                                                                       .withQueryId(UUID.randomUUID().toString())
                                                                       .withIsDirectory(true)
                                                                       .build();

    protected static final String TEST_QUERY_ID = "queryId";

    protected static AmazonDynamoDB ddbClient;

    private BlockAllocator allocator;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private DynamoDBRecordHandler handler;
    private DynamoDBMetadataHandler metadataHandler;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    private AWSGlue glueClient;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void beforeClass(){
        ddbClient = AmazonDynamoDBClient.builder()
                                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                                        .withRegion(Regions.US_WEST_1)
                                        .build();

        DynamoDB ddb = new DynamoDB(ddbClient);

        //

    }

    @Before
    public void before(){
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBRecordHandler(ddbClient,
                                            mock(AmazonS3.class),
                                            mock(AWSSecretsManager.class),
                                            mock(AmazonAthena.class),
                                            "source_type");
        metadataHandler = new DynamoDBMetadataHandler(new LocalKeyFactory(),
                                                      secretsManager,
                                                      athena,
                                                      "spillBucket",
                                                      "spillPrefix",
                                                      ddbClient,
                                                      glueClient);

    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void testReadScanSplit()
            throws Exception
    {

        String testTableName = "basic_test_table";
        String testCatalogName = "dynamodb_athena_connector";

        Split split = Split.newBuilder(SPILL_LOCATION, keyFactory.create())
                           .add(TABLE_METADATA, testTableName)
                           .add(SEGMENT_ID_PROPERTY, "0")
                           .add(SEGMENT_COUNT_METADATA, "1")
                           .build();

        TableName tableName = new TableName(testCatalogName, testTableName);

        Schema schema = metadataHandler.doGetTable(allocator,
                                                   new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID,
                                                                       testCatalogName,
                                                                       tableName))
                                       .getSchema();

        ReadRecordsRequest request = new ReadRecordsRequest(
                TEST_IDENTITY,
                testCatalogName,
                TEST_QUERY_ID,
                tableName,
                schema,
                split,
                new Constraints(ImmutableMap.of()),
                100_000_000_000L, // too big to spill
                100_000_000_000L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadScanSplit: rows[{}]", response.getRecordCount());

        assertEquals(1000, response.getRecords().getRowCount());
        logger.info("testReadScanSplit: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

}
