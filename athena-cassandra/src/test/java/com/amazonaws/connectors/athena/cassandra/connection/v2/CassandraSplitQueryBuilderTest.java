package com.amazonaws.connectors.athena.cassandra.connection.v2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.connectors.athena.cassandra.CassandraMetadataHandler;
import com.amazonaws.connectors.athena.cassandra.connection.v2.CassandraSplitQueryBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class CassandraSplitQueryBuilderTest {


    /*
        shared table, schema, etc. for tests
     */
    TableName tableName;
    Schema schema;
    Split split;
    CqlSession cqlSession;


    @Before
    public void setup(){
        tableName = new TableName("test_schema", "test_table_1");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.DATEDAY.getType()).build());
        schema = schemaBuilder.build();

        split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties())
                .thenReturn(ImmutableMap.of("partition_schema_name", "test_schema",
                        "partition_name", "test_partition_1"));
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("test_schema");
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("test_partition_1");

        cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());
    }

    // select x, y, z from table
    @Test
    public void simpleQueryTest() {

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, null, split);

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1";

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery, ((SimpleStatement) statement).getQuery());

    }

    // select x, y, z from table where x = [int, string, date, boolean]
    @Test
    public void simpleQueryWithOneConditionTest() {

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, null, split);

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1";

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery, ((SimpleStatement) statement).getQuery());

    }

    // select x, y, z from table where x [is null | is not null]
    @Test
    public void simpleQueryWithIsAndIsNotNull() {

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        ValueSet equalNull = getSingleValueSet(null);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", equalNull)
                .build());

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1 where test_col_1 is null";

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery, ((SimpleStatement) statement).getQuery());

    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

}
