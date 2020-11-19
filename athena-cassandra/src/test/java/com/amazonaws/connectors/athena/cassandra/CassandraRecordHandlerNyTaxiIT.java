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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.*;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperUtil2;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.connectors.athena.cassandra.connection.KeyspacesSessionConfig;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * NY Taxi Fares
 * <p>
 * CREATE TABLE fares (
 * id UUID PRIMARY KEY,
 * medallion text,
 * hack_license text,
 * vendor_id varchar,
 * pickup_datetime timestamp,
 * payment_type varchar,
 * fare_amount decimal,
 * surcharge decimal,
 * mta_tax decimal,
 * tip_amount decimal,
 * tolls_amount decimal,
 * total_amount decimal );
 */

public class CassandraRecordHandlerNyTaxiIT
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraRecordHandlerNyTaxiIT.class);

    private AmazonS3 amazonS3;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;

    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    private final List<CassandraRecordHandlerNyTaxiIT.ByteHolder> mockS3Storage = new ArrayList<>();
    private BlockAllocatorImpl allocator;

    private RecordService recordService;
    private S3BlockSpillReader spillReader;

    private Schema schemaForRead;

    @Before
    public void setUp()
    {
        System.out.println("setUpBefore - enter");

        try (CqlSession cqlSession = CqlSession.builder().build()) {

            schemaForRead = SchemaBuilder.newBuilder()
                                         .addField("event_id", Types.MinorType.VARBINARY.getType())
                                         .addField("medallion", Types.MinorType.VARCHAR.getType())
                                         .addField("hack_license", Types.MinorType.VARCHAR.getType())
                                         .addField("vendor_id", Types.MinorType.VARCHAR.getType())
                                         // .addField("pickup_datetime", Types.MinorType.DATEMILLI.getType())
                                         .addField("payment_type", Types.MinorType.VARCHAR.getType())
                                         .addField("fare_amount", new ArrowType.Decimal(10, 2))
                                         .addField("surcharge", new ArrowType.Decimal(10, 2))
                                         .addField("mta_tax", new ArrowType.Decimal(10, 2))
                                         .addField("tip_amount", new ArrowType.Decimal(10, 2))
                                         .addField("tolls_amount", new ArrowType.Decimal(10, 2))
                                         .addField("total_amount", new ArrowType.Decimal(10, 2))
                                         .addField("hack_license", Types.MinorType.VARCHAR.getType())
                                         .build();

            allocator = new BlockAllocatorImpl();

            amazonS3 = mock(AmazonS3.class);
            awsSecretsManager = mock(AWSSecretsManager.class);
            athena = mock(AmazonAthena.class);

            when(amazonS3.putObject(anyObject(), anyObject(), anyObject(), anyObject()))
                    .thenAnswer(invocationOnMock -> {
                        InputStream inputStream = (InputStream) invocationOnMock.getArguments()[2];
                        ByteHolder byteHolder = new ByteHolder();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        mockS3Storage.add(byteHolder);
                        return mock(PutObjectResult.class);
                    });

            when(amazonS3.getObject(anyString(), anyString()))
                    .thenAnswer(invocationOnMock -> {
                        S3Object mockObject = mock(S3Object.class);
                        ByteHolder byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        when(mockObject.getObjectContent()).thenReturn(
                                new S3ObjectInputStream(
                                        new ByteArrayInputStream(byteHolder.getBytes()), null));
                        return mockObject;
                    });

            recordService = new CassandraRecordHandlerNyTaxiIT.LocalHandler(cqlSession, allocator, amazonS3,
                                                                            awsSecretsManager, athena);
            spillReader = new S3BlockSpillReader(amazonS3, allocator);

            System.out.println("setUpBefore - exit");
        }
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doReadRecordsNoSpill()
    {
        logger.info("doReadRecordsNoSpill: enter");
        for (int i = 0; i < 2; i++) {
            EncryptionKey encryptionKey = (i % 2 == 0) ? keyFactory.create() : null;
            logger.info("doReadRecordsNoSpill: Using encryptionKey[" + encryptionKey + "]");

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("fare_amount", SortedRangeSet.copyOf(new ArrowType.Decimal(10, 6),
                                                                    ImmutableList.of(Range.greaterThan(allocator,
                                                                                                       new ArrowType.Decimal(
                                                                                                               10, 6),
                                                                                                       40.0D)), false));

            ReadRecordsRequest request = new ReadRecordsRequest(IdentityUtil.fakeIdentity(),
                                                                "catalog",
                                                                "queryId-" + System.currentTimeMillis(),
                                                                new TableName("nytaxi", "fares"),
                                                                schemaForRead,
                                                                Split.newBuilder(makeSpillLocation(),
                                                                                 encryptionKey).build(),
                                                                new Constraints(constraintsMap),
                                                                100_000_000_000L, //100GB don't expect this to spill
                                                                100_000_000_000L
            );
            //ObjectMapperUtil2.assertSerialization(request);

            RecordResponse rawResponse = recordService.readRecords(request);
            ObjectMapperUtil2.assertSerialization(rawResponse);

            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
            System.out.println(String.format("doReadRecordsNoSpill: rows[%s]", response.getRecordCount()));

            //assertTrue(response.getRecords().getRowCount() == 1);
            System.out.println("row count: " + response.getRecords().getRowCount());
            System.out.println(
                    String.format("doReadRecordsNoSpill: {%s}", BlockUtils.rowToString(response.getRecords(), 0)));
        }
        System.out.println("doReadRecordsNoSpill: exit");
    }

    private static class LocalHandler
            implements RecordService
    {
        private CassandraRecordHandler handler;
        private final BlockAllocatorImpl allocator;

        public LocalHandler(CqlSession cqlSession, BlockAllocatorImpl allocator, AmazonS3 amazonS3,
                            AWSSecretsManager secretsManager, AmazonAthena athena)
        {
            Map<String,String> env = System.getenv();
            AWSCredentials credentials = new BasicAWSCredentials(env.get("AWS_ACCESS_KEY_ID"), env.get("AWS_SECRET_ACCESS_KEY"));

            handler = new CassandraRecordHandler(cqlSession,
                                                 amazonS3,
                                                 secretsManager,
                                                 athena,
                                                 "cassandra",
                                                 new KeyspacesSessionConfig(credentials,"us-west-1"));
            //handler.setNumRows(20_000);//lower number for faster unit tests vs integ tests
            this.allocator = allocator;
        }

        @Override
        public RecordResponse readRecords(RecordRequest request)
        {

            try {
                switch (request.getRequestType()) {
                    case READ_RECORDS:
                        ReadRecordsRequest req = (ReadRecordsRequest) request;
                        RecordResponse response = handler.doReadRecords(allocator, req);
                        return response;
                    default:
                        throw new RuntimeException("Unknown request type " + request.getRequestType());
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private SpillLocation makeSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                              .withBucket("aqf-cassandra-connector-gmsharpe-test")
                              .withPrefix("lambda-spill")
                              .withQueryId(UUID.randomUUID().toString())
                              .withSplitId(UUID.randomUUID().toString())
                              .withIsDirectory(true)
                              .build();
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
