package com.amazonaws.connectors.athena.cassandra.connection.v2;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;

// For DML queries, such as SELECT
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class CassandraSplitQueryBuilder {

    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param cqlSession CQL Session. See {@link CqlSession}.
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws
     */
    public Statement buildSql(
            final CqlSession cqlSession,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
    {

        SelectFrom selectFrom =
                selectFrom(schema, table);

        // append relevant columns from split to 'SelectFrom'
        List<String> columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(col -> !split.getProperties().containsKey(col))
                .collect(Collectors.toList());

        Select select = selectFrom.columns(columnNames);

        Statement statement = select.build();

        return statement;
    }

    private List<Selector> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator)
    {
        List<Selector> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
    }

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private Selector toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", columnName);
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private Selector toPredicate(String columnName, String operator, Object value, ArrowType type,
                               List<TypeAndValue> accumulator)
    {
        accumulator.add(new TypeAndValue(type, value));
        return new DefaultRaw(columnName + " " + operator + " " + value);
    }

    private static class TypeAndValue
    {
        private final ArrowType type;
        private final Object value;

        TypeAndValue(ArrowType type, Object value)
        {
            this.type = Validate.notNull(type, "type is null");
            this.value = Validate.notNull(value, "value is null");
        }

        ArrowType getType()
        {
            return type;
        }

        Object getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "TypeAndValue{" +
                    "type=" + type +
                    ", value=" + value +
                    '}';
        }
    }

}
