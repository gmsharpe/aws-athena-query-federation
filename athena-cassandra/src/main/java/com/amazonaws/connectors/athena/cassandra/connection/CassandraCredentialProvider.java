package com.amazonaws.connectors.athena.cassandra.connection;

public interface CassandraCredentialProvider {

    CassandraCredentials getCredential();

}
