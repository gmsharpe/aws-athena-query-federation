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
