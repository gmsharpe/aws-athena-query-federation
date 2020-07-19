package com.amazonaws.connectors.athena.cassandra;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link CassandraMetadataHandler} and {@link CassandraRecordHandler}.
 */
public class CassandraCompositeHandler
        extends CompositeHandler
{
    public CassandraCompositeHandler(MetadataHandler metadataHandler, RecordHandler recordHandler) {
        super(new CassandraMetadataHandler(), new CassandraRecordHandler());
    }
}
