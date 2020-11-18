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
package com.amazonaws.connectors.athena.cassandra.connection;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.commons.lang3.Validate;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class CassandraSessionConfig {

    private String catalog;
    private final DefaultCassandraSessionFactory.Type type;
    private final String connectionString;
    private String secret;

    List<InetSocketAddress> contactPoints;
    DriverConfigLoader loader;
    /**
     * which are supported?
     * SigV4CredentialProvider
     *
     */
    Supplier<AuthProvider> authProvider;

    public List<InetSocketAddress> getContactPoints() { return contactPoints; }

    public DriverConfigLoader getLoader() { return loader; }

    public Supplier<AuthProvider> getAuthProvider() { return authProvider; }

    /**
     * Creates configuration for credentials managed by AWS Secrets Manager.
     *
     * @param catalog catalog name passed by Athena.
     * @param type Cassandra type. See {@link DefaultCassandraSessionFactory.Type}.
     * @param connectionString cassandra database connection string.
     * @param secret AWS Secrets Manager secret name.
     */
    public CassandraSessionConfig(final String catalog,
                                  final DefaultCassandraSessionFactory.Type type,
                                  final String connectionString,
                                  final String secret)
    {
        this.catalog = Validate.notBlank(catalog, "catalog must not be blank");
        this.type = Validate.notNull(type, "type must not be blank");
        this.connectionString = Validate.notBlank(connectionString, "cassandraConnectionString must not be blank");
        this.secret = Validate.notBlank(secret, "secret must not be blank");
    }

    public CassandraSessionConfig()
    {
        connectionString = "";  // empty string signals a 'localhost' Cassandra instance is expected
        type = CassandraSessionFactory.Type.DEFAULT;
    }

    /**
     * Creates configuration for credentials passed through Cassandra connection string.
     *
     * @param catalog catalog name passed by Athena.
     * @param type Cassandra type. See {@link DefaultCassandraSessionFactory.Type}.
     * @param connectionString cassandra database connection string.
     */
    public CassandraSessionConfig(final String catalog,
                                  final DefaultCassandraSessionFactory.Type type,
                                  final String connectionString)
    {
        this.catalog = Validate.notBlank(catalog, "catalog must not be blank");
        this.type = Validate.notNull(type, "type must not be blank");
        this.connectionString = Validate.notBlank(connectionString, "connectionString must not be blank");
    }

    public DefaultCassandraSessionFactory.Type getType()
    {
        return type;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getCatalog()
    {
        return catalog;
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
        return Objects.equals(getCatalog(), that.getCatalog()) &&
                getType() == that.getType() &&
                Objects.equals(getConnectionString(), that.getConnectionString()) &&
                Objects.equals(getSecret(), that.getSecret());
    }

    public static final CassandraSessionConfig DEFAULT_SESSION_CONFIG = new CassandraSessionConfig();

    public static CassandraSessionConfig getDefaultSessionConfig() {
        return DEFAULT_SESSION_CONFIG;
    }

    public static class Builder {
        private String catalog;
        private DefaultCassandraSessionFactory.Type type;
        private String connectionString;
        private String secret;

        List<InetSocketAddress> contactPoints;
        DriverConfigLoader loader;
        AuthProvider authProvider;

        public CassandraSessionConfig build(){
            return new CassandraSessionConfig();
        }
    }

    public static Builder builder(){
        return new Builder();
    }


}
