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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.amazonaws.regions.Regions.US_WEST_1;

public class KeyspacesSessionFactory implements CassandraSessionFactory
{
    static Region DEFAULT_REGION = Region.getRegion(US_WEST_1);
    static int DEFAULT_PORT = 9142;

    public KeyspacesSessionFactory(String region){
        this.region = (region != null) ? Region.getRegion(Regions.fromName(region)) : DEFAULT_REGION;
        authProvider = this::defaultAuthProvider;
    }

    public KeyspacesSessionFactory(CassandraSessionConfig cassandraSessionConfig){
        if(cassandraSessionConfig instanceof KeyspacesSessionConfig){
            KeyspacesSessionConfig keyspacesSessionConfig = ((KeyspacesSessionConfig)cassandraSessionConfig);
            region = ((KeyspacesSessionConfig)cassandraSessionConfig).getRegion();
            authProvider = () -> authProvider(keyspacesSessionConfig.getCredentials());
        }
        else {
            region = DEFAULT_REGION;
            authProvider = this::defaultAuthProvider;
        }

        this.cassandraSessionConfig = cassandraSessionConfig;


    }

    CassandraSessionConfig cassandraSessionConfig;
    Supplier<AuthProvider> authProvider;
    final Region region;

    @Override
    public CqlSession getSession()
    {
        return CqlSession.builder()
                         .withAuthProvider(defaultAuthProvider())
                         .addContactPoints(contactPoints.apply(region))
                         .withLocalDatacenter(region.getName())
                         .build();
    }

    @Override
    public CqlSession getSession(Supplier<AuthProvider> authProvider)
    {
        return CqlSession.builder()
                         .withAuthProvider(authProvider.get())
                         .addContactPoints(contactPoints.apply(region))
                         .withLocalDatacenter(region.getName())
                         .build();
    }

    static final Function<Region, List<InetSocketAddress>> contactPoints = region ->
            Collections.singletonList(new InetSocketAddress(String.format("cassandra.%s.amazonaws.com", region.getName()), DEFAULT_PORT));

    public AuthProvider authProvider(AWSCredentials credentials){
        return new SigV4AuthProvider(new AWSStaticCredentialsProvider(credentials), region.getName());
    }

    private AuthProvider defaultAuthProvider(){
        return new SigV4AuthProvider(new DefaultAWSCredentialsProviderChain(), region.getName());
    }

    static final List<String> SUPPORTED_REGIONS = Collections.singletonList(US_WEST_1.getName());

}
