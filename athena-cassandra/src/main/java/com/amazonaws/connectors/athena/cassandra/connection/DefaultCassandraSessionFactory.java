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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;

import java.util.function.Supplier;

public class DefaultCassandraSessionFactory implements CassandraSessionFactory
{

    private final CassandraSessionConfig cassandraSessionConfig;

    public DefaultCassandraSessionFactory(final CassandraSessionConfig cassandraSessionConfig){
        this.cassandraSessionConfig = cassandraSessionConfig;
    }


    @Override
    public CqlSession getSession() {
        return CqlSession.builder().build();
    }


    public static DefaultCassandraSessionFactory getDefaultSessionFactory() {
        return new DefaultCassandraSessionFactory(CassandraSessionConfig.getDefaultSessionConfig());
    }

    @Override
    public CqlSession getSession(Supplier<AuthProvider> authProvider){
        return CqlSession.builder().build();
    }


}
