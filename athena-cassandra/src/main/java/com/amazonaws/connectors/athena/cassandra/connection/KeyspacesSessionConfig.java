package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class KeyspacesSessionConfig extends CassandraSessionConfig
{

    public KeyspacesSessionConfig(String region){
        super(CassandraSessionFactory.Type.AWS_KEYSPACES);
        this.region = Region.getRegion(Regions.fromName(region));
    }

    public KeyspacesSessionConfig(AWSCredentials credentials, String region){
        super(CassandraSessionFactory.Type.AWS_KEYSPACES);
        this.region = Region.getRegion(Regions.fromName(region));
        this.credentials = credentials;
    }

    Region region;
    AWSCredentials credentials;

    public Region getRegion(){
        return region;
    }

    public AWSCredentials getCredentials(){
        return credentials;
    }

}
