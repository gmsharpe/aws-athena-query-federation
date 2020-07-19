package com.amazonaws.connectors.athena.cassandra.connection;

import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraSessionFactory {

    private final CassandraSessionConfig cassandraSessionConfig;

    public CassandraSessionFactory(final CassandraSessionConfig cassandraSessionConfig){
        this.cassandraSessionConfig = cassandraSessionConfig;
    }

    public CqlSession getSession() {
        return CqlSession.builder().build();
    }

    public static CassandraSessionFactory getDefaultSessionFactory() {
        return new CassandraSessionFactory(CassandraSessionConfig.getDefaultSessionConfig());
    }

    public CqlSession getSession(CassandraCredentialProvider cassandraCredentialProvider){

        return CqlSession.builder().build();

    }

}
