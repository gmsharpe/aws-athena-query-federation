package com.amazonaws.connectors.athena.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SyncCqlSession;
import org.junit.Before;
import org.junit.Test;

public class CassandraMetadataHandlerTest {

    CqlSession cqlSession;

    @Before
    public void setup(){

        cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());
    }

    /**
     * Since v 6.0 Docs
     *
     * Get keyspaces info
     *
     * SELECT * FROM system_schema.keyspaces
     *
     * Get tables info
     *
     * SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name';
     *
     * Get table info
     *
     * SELECT * FROM system_schema.columns
     * WHERE keyspace_name = 'keyspace_name' AND table_name = 'table_name';
     */

    // TESTS WITH CASSANDRA ON LOCALHOST
    // https://docs.datastax.com/en/pdf/osscql3x.pdf
    @Test
    public void doListTables(){
        //ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'keyspace name'");
        ResultSet resultSet = cqlSession.execute("SELECT * FROM system_schema.keyspaces");

        resultSet = cqlSession.execute("SELECT * FROM system_schema.tables");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));


        resultSet = cqlSession.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'simplex';");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));

        resultSet = cqlSession.execute("SELECT * FROM system_schema.columns WHERE keyspace_name = 'simplex';");
        resultSet.forEach(r -> System.out.println(r.getFormattedContents()));
        //System.out.println(resultSet.getColumnDefinitions());
    }

}
