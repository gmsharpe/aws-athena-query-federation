package com.amazonaws.connectors.athena.cassandra.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;

import java.util.function.Supplier;

public interface CassandraSessionFactory
{

    CqlSession getSession(Supplier<AuthProvider> authProvider);

    /**
     * connect to Cassandra on 'localhost' with no authentication on default port
     */
    default CqlSession getSession()
    {
        return CqlSession.builder().build();
    }

    enum Type {

        AWS_KEYSPACES("aws_keyspaces"),
        DEFAULT("default"),
        LOCALHOST("localhost");

        private final String type;

        Type(final String type)
        {
            this.type = type;
        }

        public String getType()
        {
            return type;
        }

    }
}
