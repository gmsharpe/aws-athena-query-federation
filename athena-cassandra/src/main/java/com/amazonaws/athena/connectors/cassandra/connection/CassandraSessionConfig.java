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
package com.amazonaws.athena.connectors.cassandra.connection;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.commons.lang3.Validate;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static com.amazonaws.athena.connectors.cassandra.connection.CassandraSessionFactory.*;

public class CassandraSessionConfig
{
    private String keyspace;
    private final Type type;
    private final String connectionString;
    private String secret;

    enum SessionProps
    {
        KEYSPACE,
        TYPE
    }

    List<InetSocketAddress> contactPoints;
    DriverConfigLoader loader;
    /**
     * which are supported?
     * SigV4CredentialProvider
     */
    Supplier<AuthProvider> authProvider;

    public List<InetSocketAddress> getContactPoints()
    {
        return contactPoints;
    }

    public DriverConfigLoader getLoader()
    {
        return loader;
    }

    public Supplier<AuthProvider> getAuthProvider()
    {
        return authProvider;
    }

    /**
     * Creates configuration for credentials managed by AWS Secrets Manager.
     *
     * @param keyspace         keyspace name passed by Athena.
     * @param type             Cassandra type. See {@link DefaultCassandraSessionFactory.Type}.
     * @param connectionString cassandra database connection string.
     * @param secret           AWS Secrets Manager secret name.
     */
    public CassandraSessionConfig(final String keyspace,
                                  final Type type,
                                  final String connectionString,
                                  final String secret)
    {
        this.keyspace = keyspace;
        this.type = Validate.notNull(type, "type must not be blank");
        this.connectionString = connectionString;
        this.secret = secret;
    }

    public CassandraSessionConfig()
    {
        connectionString = "";  // empty string signals a 'localhost' Cassandra instance is expected
        type = Type.DEFAULT;
    }

    public CassandraSessionConfig(final Type type)
    {
        this.type = type;
        connectionString = "";
    }

    /**
     * Creates configuration for credentials passed through Cassandra connection string.
     *
     * @param keyspace         keyspace name passed by Athena.
     * @param type             Cassandra type. See {@link DefaultCassandraSessionFactory.Type}.
     * @param connectionString cassandra database connection string.
     */
    public CassandraSessionConfig(final String keyspace,
                                  final DefaultCassandraSessionFactory.Type type,
                                  final String connectionString)
    {
        this.keyspace = keyspace;
        this.type = Validate.notNull(type, "type must not be blank");
        this.connectionString = connectionString;
    }

    public Type getType()
    {
        return type;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getSecret()
    {
        return secret;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraSessionConfig that = (CassandraSessionConfig) o;
        return Objects.equals(getKeyspace(), that.getKeyspace()) &&
                getType() == that.getType() &&
                Objects.equals(getConnectionString(), that.getConnectionString()) &&
                Objects.equals(getSecret(), that.getSecret());
    }

    public static final CassandraSessionConfig DEFAULT_SESSION_CONFIG = new CassandraSessionConfig();

    public static CassandraSessionConfig getDefaultSessionConfig()
    {
        return DEFAULT_SESSION_CONFIG;
    }

    public static CassandraSessionConfig buildSessionConfigFromEnv()
    {
        Map<String, String> properties = System.getenv();
        Type type = Type.valueOf(properties.getOrDefault(SessionProps.TYPE.toString(), Type.AWS_KEYSPACES.toString()));
        String keyspace = properties.get(SessionProps.KEYSPACE.toString());

        return CassandraSessionConfig.builder()
                                     .withType(type)
                                     .withKeyspace(keyspace)
                                     .build();
    }

    public static class Builder
    {

        private String keyspace;
        private Type type;
        private String connectionString;
        private String secret;

        List<InetSocketAddress> contactPoints;
        DriverConfigLoader loader;
        AuthProvider authProvider;

        public CassandraSessionConfig build()
        {
            return new CassandraSessionConfig(keyspace, type, connectionString, secret);
        }

        public String getSecret()
        {
            return secret;
        }

        public Builder withSecret(String secret)
        {
            this.secret = secret;
            return this;
        }

        public String getKeyspace()
        {
            return keyspace;
        }

        public Builder withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public Type getType()
        {
            return type;
        }

        public Builder withType(Type type)
        {
            this.type = type;
            return this;
        }

        public String getConnectionString()
        {
            return connectionString;
        }

        public Builder withConnectionString(String connectionString)
        {
            this.connectionString = connectionString;
            return this;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }
}
