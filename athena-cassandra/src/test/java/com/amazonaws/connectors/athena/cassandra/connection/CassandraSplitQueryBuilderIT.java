package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.connectors.athena.cassandra.CassandraMetadataHandler;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class CassandraSplitQueryBuilderTest {

    // select col_1, col_2 from test_table_1 where attr_1 <= 100

    // see PostGreSqlRecordHandlerTest.buildSplitSql()

    @Test
    public void testBuildSplit(){
        TableName tableName = new TableName("test_schema", "test_table_1");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("test_col_3", Types.MinorType.DATEDAY.getType()).build());
        Schema schema = schemaBuilder.build();

        CassandraSplitQueryBuilder queryBuilder = new CassandraSplitQueryBuilder();

        CqlSession cqlSession;// = Mockito.mock(CqlSession.class);
        cqlSession = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", cqlSession.getName());
        /*
         final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split
         */

        String catalog = "test_catalog";

        ValueSet testCol1 = Mockito.mock(EquatableValueSet.class);
        ValueSet testCol2 = getRangeSet(Marker.Bound.ABOVE,1, Marker.Bound.BELOW,10);
        ValueSet testCol3 = getRangeSet(Marker.Bound.EXACTLY,5, Marker.Bound.EXACTLY,50);

        Constraints constraints = new Constraints(new ImmutableMap.Builder<String, ValueSet>()
                .put("test_col_1", testCol1)
                .put("test_col_2", testCol2)
                .put("test_col_3", testCol3).build());

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of("partition_schema_name", "s0", "partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("s0");
        Mockito.when(split.getProperty(Mockito.eq(CassandraMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");

        PreparedStatement stmt = queryBuilder.buildSql(cqlSession, catalog, tableName.getSchemaName(),tableName.getTableName(), schema, constraints, split);

        System.out.println(stmt.getQuery());

    }


    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

}
