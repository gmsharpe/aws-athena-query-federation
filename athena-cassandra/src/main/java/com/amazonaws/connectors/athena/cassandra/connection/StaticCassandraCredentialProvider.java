package com.amazonaws.connectors.athena.cassandra.connection;

public class StaticCassandraCredentialProvider implements CassandraCredentialProvider {

    @Override
    public CassandraCredentials getCredential() {
        return null;
    }
}
