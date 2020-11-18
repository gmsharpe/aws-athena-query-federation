package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.amazonaws.regions.Regions.US_WEST_1;

public class KeyspacesSessionFactory implements CassandraSessionFactory
{

    private static final String DEFAULT_REGION = "us-west-1";

    public KeyspacesSessionFactory(String region){
        this.region = Region.getRegion(Regions.fromName((region != null) ? region : DEFAULT_REGION));
        authProvider = this::defaultAuthProvider;
    }

    public KeyspacesSessionFactory(CassandraSessionConfig cassandraSessionConfig,
                                   String region){
        this.cassandraSessionConfig = cassandraSessionConfig;
        this.region = Region.getRegion(Regions.fromName((region != null) ? region : DEFAULT_REGION)); // also serves to validate region provided, if one exists
        authProvider = this::defaultAuthProvider;
    }

    CassandraSessionConfig cassandraSessionConfig;
    Supplier<AuthProvider> authProvider;
    final Region region;

    @Override
    public CqlSession getSession()
    {
        return CqlSession.builder()
                         .addContactPoints(contactPoints.apply(region))
                         .withLocalDatacenter(region.getName())
                         .build();
    }

    @Override
    public CqlSession getSession(Supplier<AuthProvider> authProvider)
    {
        return CqlSession.builder()
                         .addContactPoints(contactPoints.apply(region))
                         .withLocalDatacenter(region.getName())
                         .build();
    }

    static final Function<Region, List<InetSocketAddress>> contactPoints = region ->
            Collections.singletonList(new InetSocketAddress(String.format("cassandra.%s.amazonaws.com", region.getName()), 9142));

    private AuthProvider defaultAuthProvider(){
        return new SigV4AuthProvider(new DefaultAWSCredentialsProviderChain(), region.getName());
    }

    static final List<String> SUPPORTED_REGIONS = Collections.singletonList(US_WEST_1.getName());

}
