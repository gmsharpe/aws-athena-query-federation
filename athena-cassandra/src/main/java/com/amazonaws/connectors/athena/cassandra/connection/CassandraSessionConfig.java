package com.amazonaws.connectors.athena.cassandra.connection;

public class CassandraSessionConfig {


    public static final CassandraSessionConfig DEFAULT_SESSION_CONFIG = new CassandraSessionConfig();

    public static CassandraSessionConfig getDefaultSessionConfig() {
        return DEFAULT_SESSION_CONFIG;
    }

}
