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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.aws.mcs.auth.SigV4AuthProvider;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.amazonaws.regions.Regions.US_WEST_1;

public class KeyspacesSessionFactory implements CassandraSessionFactory
{
    static Region DEFAULT_REGION = Region.getRegion(US_WEST_1);
    static int DEFAULT_PORT = 9142;
    private static Logger LOGGER = LoggerFactory.getLogger(KeyspacesSessionFactory.class);

    private static CqlSession cqlSession = null;

    KeyspacesSessionFactory()
    {
        Region fromEnv = getRegionFromSystemEnv();
        this.region = (fromEnv != null) ? fromEnv : DEFAULT_REGION;
        authProvider = this::defaultAuthProvider;
    }

    public KeyspacesSessionFactory(String region)
    {
        this.region = (region != null) ? Region.getRegion(Regions.fromName(region)) : DEFAULT_REGION;
        authProvider = this::defaultAuthProvider;
    }

    public KeyspacesSessionFactory(CassandraSessionConfig cassandraSessionConfig)
    {
        if (cassandraSessionConfig instanceof KeyspacesSessionConfig) {
            KeyspacesSessionConfig keyspacesSessionConfig = ((KeyspacesSessionConfig) cassandraSessionConfig);
            region = ((KeyspacesSessionConfig) cassandraSessionConfig).getRegion();

            if (keyspacesSessionConfig.getCredentials() != null) {
                authProvider = () -> authProvider(keyspacesSessionConfig.getCredentials());
            } else {
                authProvider = this::defaultAuthProvider;
            }
        } else {
            region = getRegionFromSystemEnv();
            authProvider = this::defaultAuthProvider;
        }

        this.cassandraSessionConfig = cassandraSessionConfig;
    }

    CassandraSessionConfig cassandraSessionConfig;
    Supplier<AuthProvider> authProvider;
    final Region region;

    public static Region getRegionFromSystemEnv()
    {
        String region = System.getenv("AWS_REGION");
        return (region != null) ? Region.getRegion(Regions.fromName(region)) : DEFAULT_REGION;
    }

    @Override
    public CqlSession getSession()
    {
        if(cqlSession == null || cqlSession.isClosed()) {

            try {

                // https://community.datastax.com/questions/6902/how-to-overcome-connection-timeouts-when-connectin.html

                DriverConfigLoader loader =
                        DriverConfigLoader.programmaticBuilder().withString(DefaultDriverOption.PROTOCOL_VERSION,
                                                                            ProtocolVersion.V4.name())
                                          .withString(DefaultDriverOption.REQUEST_CONSISTENCY,
                                                      DefaultConsistencyLevel.ONE.name())
                                          .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(5))
                                          .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
                                          .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(5))
                                          .withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, Duration.ofSeconds(5))
                                          .build();

                cqlSession = CqlSession.builder()
                                 .withAuthProvider(defaultAuthProvider())
                                 .withSslContext(SSLContext.getDefault())
                                 .withConfigLoader(loader)
                                 .addContactPoints(contactPoints.apply(region))
                                 .withLocalDatacenter(region.getName())
                                 .build();

                return cqlSession;
            }
            catch (RuntimeException e) {
                LOGGER.error("Unable to create CqlSession", e);
                throw new RuntimeException(e);
            }
            catch (NoSuchAlgorithmException e) {
                LOGGER.error("Unable to create CqlSession", e);
                throw new RuntimeException(e);
            }
        }
        else {
            return cqlSession;
        }
    }

    @Override
    public CqlSession getSession(Supplier<AuthProvider> authProvider)
    {
        try {
            return CqlSession.builder()
                             .withAuthProvider(authProvider.get())
                             .addContactPoints(contactPoints.apply(region))
                             .withSslContext(SSLContext.getDefault())
                             .withLocalDatacenter(region.getName())
                             .build();
        }
        catch (NoSuchAlgorithmException e) {
            LOGGER.error("Unable to create CqlSession", e);
            throw new RuntimeException(e);
        }
    }

    static final Function<Region, List<InetSocketAddress>> contactPoints = region ->
            Collections.singletonList(
                    new InetSocketAddress(String.format("cassandra.%s.amazonaws.com", region.getName()), DEFAULT_PORT));

    public AuthProvider authProvider(AWSCredentials credentials)
    {
        return new SigV4AuthProvider(new AWSStaticCredentialsProvider(credentials), region.getName());
    }

    private AuthProvider defaultAuthProvider()
    {
        return new SigV4AuthProvider(new DefaultAWSCredentialsProviderChain(), region.getName());
    }

    static final List<String> SUPPORTED_REGIONS = Collections.singletonList(US_WEST_1.getName());

    public static KeyspacesSessionFactory getInstance()
    {
        return new KeyspacesSessionFactory();
    }
}
