package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.connectors.athena.cassandra.CassandraMetadataHandler;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class CassandraSplitQueryBuilder2Test {

    // select x, y, z from table
    @Test
    public void simpleQueryTest() {

        TableName tableName = new TableName("test_schema", "test_table_1");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();

        CassandraSplitQueryBuilder2 queryBuilder = new CassandraSplitQueryBuilder2();

        CqlSession cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties())
                .thenReturn(ImmutableMap.of("partition_schema_name", "test_schema",
                        "partition_name", "test_partition_1"));
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("test_schema");
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("test_partition_1");

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, null, split);

        String expectedQuery = "SELECT * FROM test_schema.test_table_1";

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery, ((SimpleStatement) statement).getQuery());

    }


}
