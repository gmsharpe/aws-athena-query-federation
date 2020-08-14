package com.amazonaws.connectors.athena.cassandra.connection;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.connectors.athena.cassandra.CassandraMetadataHandler;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * todo deconstruct and refactor (from JdbcSplitQueryBuilder and subclasses - PostGreSqlQueryStringBuilder, etc.)
 *
 * note: still deconstructing what this would do, using the JDBC connector example.  because of similarities with cql and sql, it's likely
 * some of the functionality from the JDBC Split Query Builder may prove useful
 */

public class CassandraSplitQueryBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSplitQueryBuilder.class);

    private static final int MILLIS_SHIFT = 12;


    private final String CASSANDRA_QUOTE_CHARACTERS = "\"";

    /**
     * https://docs.datastax.com/en/developer/java-driver/4.8/manual/query_builder/
     */

    public CassandraSplitQueryBuilder()
    {

    }

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
    public PreparedStatement buildSql(
            final CqlSession cqlSession,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columnNames.isEmpty()) {
            sql.append("null");
        }

        sql.append(getFromClauseWithSplit(catalog, schema, table, split));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(tableSchema.getFields(), constraints, accumulator, split.getProperties());
        clauses.addAll(getPartitionWhereClauses(split));
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        LOGGER.info("Generated SQL : {}", sql.toString());
        System.out.println(sql.toString());

        List<Object> positionalValues = new ArrayList<>();

        // convert accumulator TypeAndValue entries to Cassandra types
        // TODO all types, converts Arrow values to JDBC.
        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);

            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());

            switch (minorTypeForArrowType) {
                case BIGINT:
                    positionalValues.add(typeAndValue.getValue());
                    break;
                case INT:
                    positionalValues.add(((Number) typeAndValue.getValue()).intValue());
                    break;
                case SMALLINT:
                    positionalValues.add(((Number) typeAndValue.getValue()).shortValue());
                    break;
                case TINYINT:
                    positionalValues.add( ((Number) typeAndValue.getValue()).byteValue());
                    break;
                case FLOAT8:
                    positionalValues.add( (double) typeAndValue.getValue());
                    break;
                case FLOAT4:
                    positionalValues.add((float) typeAndValue.getValue());
                    break;
                case BIT:
                    positionalValues.add((boolean) typeAndValue.getValue());
                    break;
                case DATEDAY:
                    LocalDateTime dateTime = ((LocalDateTime) typeAndValue.getValue());
                    positionalValues.add(new Date(dateTime.toDateTime(DateTimeZone.UTC).getMillis()));
                    break;
                case DATEMILLI:
                    LocalDateTime timestamp = ((LocalDateTime) typeAndValue.getValue());
                    positionalValues.add(new Timestamp(timestamp.toDateTime(DateTimeZone.UTC).getMillis()));
                    break;
                case VARCHAR:
                    positionalValues.add(String.valueOf(typeAndValue.getValue()));
                    break;
                case VARBINARY:
                    positionalValues.add((byte[]) typeAndValue.getValue());
                    break;
                case DECIMAL:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) typeAndValue.getType();
                    positionalValues.add(BigDecimal.valueOf((long) typeAndValue.getValue(), decimalType.getScale()));
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType));
            }
        }

        PreparedStatement statement = cqlSession.prepare(SimpleStatement.newInstance(sql.toString()).setPositionalValues(positionalValues));

        return statement;
    }



    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(catalog)) {
            tableName.append(quote(catalog)).append('.');
        }
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));

        tableName.append(quote(table));

        String partitionSchemaName = split.getProperty(CassandraMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
        String partitionName = split.getProperty(CassandraMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);

        if (CassandraMetadataHandler.ALL_PARTITIONS.equals(partitionSchemaName) || CassandraMetadataHandler.ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            return String.format(" FROM %s ", tableName);
        }

        return String.format(" FROM %s.%s ", quote(partitionSchemaName), quote(partitionName));
    }

    // see PostgresqlSplitqueryBuilder
    protected List<String> getPartitionWhereClauses(final Split split)
    {

        return Collections.emptyList();
    }

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private List<String> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator, Map<String, String> partitionSplit)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                // todo explore the relevance/truth of this statement concerning Cassandra
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
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
    private String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
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

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
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
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    accumulator.add(new TypeAndValue(type, value));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "?"));
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    // todo deconstruct and refactor (from JdbcSplitQueryBuilder)
    private String toPredicate(String columnName, String operator, Object value, ArrowType type,
                               List<TypeAndValue> accumulator)
    {
        accumulator.add(new TypeAndValue(type, value));
        return quote(columnName) + " " + operator + " ?";
    }

    protected String quote(String name)
    {
        name = name.replace(CASSANDRA_QUOTE_CHARACTERS, CASSANDRA_QUOTE_CHARACTERS + CASSANDRA_QUOTE_CHARACTERS);
        return CASSANDRA_QUOTE_CHARACTERS + name + CASSANDRA_QUOTE_CHARACTERS;
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
