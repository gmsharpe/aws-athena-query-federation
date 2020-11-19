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

    static CassandraSessionFactory getConnectionFactory(CassandraSessionConfig sessionConfig){
        switch(sessionConfig.getType()){
            case AWS_KEYSPACES:
                return new KeyspacesSessionFactory(sessionConfig);
            default:
                return new DefaultCassandraSessionFactory(sessionConfig);
        }
    }

}
