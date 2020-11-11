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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.connectors.athena.cassandra.CassandraMetadataHandler;
import com.amazonaws.connectors.athena.cassandra.connection.CassandraSplitQueryBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/*
    docker run --name cassandra -p 9042:9042 -d cassandra:latest
 */
public class CassandraSplitQueryBuilderTest {


    /*
        shared table, schema, etc. for tests
     */
    TableName tableName;
    Schema schema;
    Split split;
    CqlSession cqlSession;
    Constraints constraints;


    @Before
    public void setup(){
        tableName = new TableName("test_schema", "test_table_1");



        split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties())
                .thenReturn(ImmutableMap.of("partition_schema_name", "test_schema",
                        "partition_name", "test_partition_1"));
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("test_schema");
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("test_partition_1");

        constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .build());


        cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());
    }

    // select x, y, z from table
    @Test
    public void simpleQueryTest() {

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.BIGINT.getType()).build());
        schema = schemaBuilder.build();

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1";


        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery, ((SimpleStatement) statement).getQuery());

    }

    // select x, y, z from table where x = [int, string, date, boolean]
    @Test
    public void simpleQueryWithOneConditionTest() {

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.BIGINT.getType()).build());

        schema = schemaBuilder.build();

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        ValueSet testCol1ValSet = getSingleValueSet(1);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", testCol1ValSet)
                .build());

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1 where test_col_1=1";

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery.toLowerCase(), ((SimpleStatement) statement).getQuery().toLowerCase());

    }

    // select x, y, z from table where x = int and y = string
    @Test
    public void simpleQueryWithTwoConditionTest() {

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.BIGINT.getType()).build());

        schema = schemaBuilder.build();

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        ValueSet testCol1ValSet = getSingleValueSet(1);
        ValueSet testCol2ValSet = getSingleValueSet("some string");
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", testCol1ValSet)
                .put("test_col_2", testCol2ValSet)
                .build());

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1 where test_col_1=1 and test_col_2='some string'";

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery.toLowerCase(), ((SimpleStatement) statement).getQuery().toLowerCase());

    }

    // select a, b, c, d, e, f from table where a = int and b = string and c = boolean and d = date .... and ? = ?
    @Test
    public void simpleQueryWithNConditionsOfManyTypesTest() {

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.BIGINT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_4", Types.MinorType.FLOAT4.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_5", Types.MinorType.SMALLINT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_6", Types.MinorType.TINYINT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_7", Types.MinorType.FLOAT8.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_8", Types.MinorType.BIT.getType()).build());
        schema = schemaBuilder.build();


        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        ValueSet testCol1ValSet = getSingleValueSet(1);
        ValueSet testCol2ValSet = getSingleValueSet("some string");
        ValueSet testCol3ValSet = getSingleValueSet(new BigInteger("1000000000000000000000",10));
        ValueSet testCol4ValSet = getSingleValueSet(1.1F);
        ValueSet testCol5ValSet = getSingleValueSet(1);
        ValueSet testCol6ValSet = getSingleValueSet(0);
        ValueSet testCol7ValSet = getSingleValueSet(1.2d);
        ValueSet testCol8ValSet = getSingleValueSet(true);



        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", testCol1ValSet)
                .put("test_col_2", testCol2ValSet)
                .put("test_col_3", testCol3ValSet)
                .put("test_col_4", testCol4ValSet)
                .put("test_col_5", testCol5ValSet)
                .put("test_col_6", testCol6ValSet)
                .put("test_col_7", testCol7ValSet)
                .put("test_col_8", testCol8ValSet)
                .build());

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3,test_col_4,test_col_5,test_col_6,test_col_7,test_col_8 " +
                "FROM test_schema.test_table_1 WHERE test_col_1=1 AND test_col_2='some string' AND test_col_3=1000000000000000000000 " +
                "AND test_col_4=1.1 AND test_col_5=1 AND test_col_6=0 AND test_col_7=1.2 AND test_col_8=true";

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        System.out.println(((SimpleStatement) statement).getQuery());
        //assertEquals(expectedQuery.toLowerCase(), ((SimpleStatement) statement).getQuery().toLowerCase());

    }

    // select x, y, z from table where x is not null
    @Test
    public void simpleQueryWithIsNotNull() {

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.BIGINT.getType()).build());

        schema = schemaBuilder.build();

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.getLow().isLowerUnbounded()).thenReturn(true);
        Mockito.when(range1a.getHigh().isUpperUnbounded()).thenReturn(true);
        SortedRangeSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.isNullAllowed()).thenReturn(false);
        Mockito.when(valueSet1.getSpan()).thenReturn(range1a);

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", valueSet1)
                .build());

        Statement statement = queryBuilder.buildSql(cqlSession, "catalog", "test_schema", "test_table_1", schema, constraints, split);

        String expectedQuery = "SELECT test_col_1,test_col_2,test_col_3 FROM test_schema.test_table_1 where test_col_1 is not null";

        System.out.println(((SimpleStatement) statement).getQuery());
        assertEquals(expectedQuery.toLowerCase(), ((SimpleStatement) statement).getQuery().toLowerCase());

    }





    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    // not working for some reason.
    private ValueSet getNotNullValueSet(){
        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.getLow().isLowerUnbounded()).thenReturn(true);
        Mockito.when(range1a.getHigh().isUpperUnbounded()).thenReturn(true);
        SortedRangeSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.isNullAllowed()).thenReturn(false);
        Mockito.when(valueSet1.getSpan()).thenReturn(range1a);
        return valueSet1;
    }

}
