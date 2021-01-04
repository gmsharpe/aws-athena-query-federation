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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RecordService;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.datastax.oss.driver.api.core.CqlSession;

import java.util.UUID;

public class AthenaQfUtils {



    public static class LocalHandler
            implements RecordService {
        private CassandraRecordHandler handler;
        private final BlockAllocatorImpl allocator;

        public LocalHandler(CqlSession cqlSession, BlockAllocatorImpl allocator, AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena) {


            handler = new CassandraRecordHandler(cqlSession, amazonS3, secretsManager, athena);
            //handler.setNumRows(20_000);//lower number for faster unit tests vs integ tests
            this.allocator = allocator;
        }

        @Override
        public RecordResponse readRecords(RecordRequest request) {

            try {
                switch (request.getRequestType()) {
                    case READ_RECORDS:
                        ReadRecordsRequest req = (ReadRecordsRequest) request;
                        RecordResponse response = handler.doReadRecords(allocator, req);
                        return response;
                    default:
                        throw new RuntimeException("Unknown request type " + request.getRequestType());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    private SpillLocation makeSpillLocation() {
        return S3SpillLocation.newBuilder()
                .withBucket("aqf-cassandra-connector-gmsharpe-test")
                .withPrefix("lambda-spill")
                .withQueryId(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }

    private class ByteHolder {
        private byte[] bytes;
        public void setBytes(byte[] bytes) {
            this.bytes = bytes;
        }
        public byte[] getBytes() {
            return bytes;
        }
    }
}
