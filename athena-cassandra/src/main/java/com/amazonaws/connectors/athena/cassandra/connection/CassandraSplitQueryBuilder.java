package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

// For DML queries, such as SELECT

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

        List<Relation> clauses = toConjuncts(tableSchema.getFields(), constraints, null);

        Statement statement = select.where(clauses)
                                    .allowFiltering()
                                    .build();
        System.out.println(((SimpleStatement) statement).getQuery());

        return statement;
    }

    private List<Relation> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator)
    {
        List<Relation> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.addAll(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
    }

    /**
     * https://docs.datastax.com/en/developer/java-driver/4.8/manual/query_builder/relation/
     *
     * @param columnName
     * @param valueSet
     * @param type
     * @param accumulator
     * @return
     */

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private List<Relation> toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {

        // https://en.wikipedia.org/wiki/Logical_disjunction
        List<Relation> relations = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return Collections.singletonList(Relation.column(columnName).isEqualTo(literal(null)));
            }

            // TODO 'or' not allowed, so need to figure out how to add disjuncts
            // https://stackoverflow.com/questions/10139390/alternative-for-or-condition-after-where-clause-in-select-statement-cassandra
            // https://thelastpickle.com/blog/2016/09/15/Null-bindings-on-prepared-statements-and-undesired-tombstone-creation.html
/*            if (valueSet.isNullAllowed()) {
                disjuncts.add(Relation.column(columnName).isEqualTo(literal(null)));
            }*/


            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return  Arrays.asList(Relation.column(columnName).isNotNull());
            }

        }

        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<Relation> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                Preconditions.checkState(!rangeConjuncts.isEmpty());
                relations.addAll(rangeConjuncts);
            }
        }

        if (singleValues.size() == 1) {
            // no way known for adding disjuncts, yet
            return Arrays.asList(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                accumulator.add(new TypeAndValue(type, value));
            }
            relations.add(Relation.column(columnName).in(singleValues.stream().map(QueryBuilder::literal).collect(Collectors.toList())));
        }

        return relations;
    }

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private Relation toPredicate(String columnName, String operator, Object value, ArrowType type,
                               List<TypeAndValue> accumulator)
    {
        //accumulator.add(new TypeAndValue(type, value));
        switch (operator) {
            case "=":
                return Relation.column(columnName).isEqualTo(literal(value));
            case ">":
                return Relation.column(columnName).isGreaterThan(literal(value));
            case ">=":
                return Relation.column(columnName).isGreaterThanOrEqualTo(literal(value));
            case "<":
                return Relation.column(columnName).isLessThan(literal(value));
            case "<=":
                return Relation.column(columnName).isLessThanOrEqualTo(literal(value));
            default:
                throw new IllegalArgumentException("unsupported operator");
        }

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
