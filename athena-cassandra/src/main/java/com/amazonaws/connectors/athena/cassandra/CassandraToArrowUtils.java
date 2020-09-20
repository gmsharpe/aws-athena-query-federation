package com.amazonaws.connectors.athena.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import static com.datastax.oss.driver.api.core.type.DataTypes.*;

import java.util.*;
import java.util.function.Supplier;

/**
 *
 * see {@code JdbcToArrowUtils}
 * see {@code JdbcArrowTypeConverter}
 *
 * from: https://cassandra.apache.org/doc/latest/cql/types.html (07/22/2020)
 *
 * NATIVE TYPES
 *
 * Data Type	Constants	        Description
 * ------------------------------------------------------------------
 * ascii	    string	            ASCII character string
 * bigint	    integer	            64-bit signed long
 * blob	        blob            	Arbitrary bytes (no validation)
 * boolean	    boolean	            Either true or false
 * counter	    integer	            Counter column (64-bit signed value). See Counters for details
 * date	        integer, string	    A date (with no corresponding time value). See Working with dates below for details
 * decimal	    integer, float	    Variable-precision decimal
 * double	    integer float	    64-bit IEEE-754 floating point
 * duration	    duration,	        A duration with nanosecond precision. See Working with durations below for details
 * float	    integer, float	    32-bit IEEE-754 floating point
 * inet	        string	            An IP address, either IPv4 (4 bytes long) or IPv6 (16 bytes long). Note that there is no inet constant, IP address should be input as strings
 * int	        integer	            32-bit signed int
 * smallint	    integer	            16-bit signed int
 * text	        string	            UTF8 encoded string
 * time	        integer, string	    A time (with no corresponding date value) with nanosecond precision. See Working with times below for details
 * timestamp	integer, string	    A timestamp (date and time) with millisecond precision. See Working with timestamps below for details
 * timeuuid	    uuid	            Version 1 UUID, generally used as a “conflict-free” timestamp. Also see Timeuuid functions
 * tinyint	    integer	            8-bit signed int
 * uuid	        uuid            	A UUID (of any version)
 * varchar	    string	            UTF8 encoded string
 * varint	    integer	            Arbitrary-precision integer
 *
 * COLLECTION TYPES
 *
 * Collection   Description
 * ----------------------------------------------------------------------
 * list	        A list is a collection of one or more ordered elements.
 * map	        A map is a collection of key-value pairs.
 * set	        A set is a collection of one or more elements.
 *
 * USER DEFINED DATATYPES
 * todo ?
 *
 *
 * @param type
 * @param position
 * @param clustering_order
 * @return
 */

/**
 * ASCII = new PrimitiveType(1);
 * BIGINT = new PrimitiveType(2);
 * BLOB = new PrimitiveType(3);
 * BOOLEAN = new PrimitiveType(4);
 * COUNTER = new PrimitiveType(5);
 * DECIMAL = new PrimitiveType(6);
 * DOUBLE = new PrimitiveType(7);
 * FLOAT = new PrimitiveType(8);
 * INT = new PrimitiveType(9);
 * TIMESTAMP = new PrimitiveType(11);
 * UUID = new PrimitiveType(12);
 * VARINT = new PrimitiveType(14);
 * TIMEUUID = new PrimitiveType(15);
 * INET = new PrimitiveType(16);
 * DATE = new PrimitiveType(17);
 * TEXT = new PrimitiveType(13);
 * TIME = new PrimitiveType(18);
 * SMALLINT = new PrimitiveType(19);
 * TINYINT = new PrimitiveType(20);
 * DURATION = new PrimitiveType(21);
 */

// todo - complex types

public class CassandraToArrowUtils {

    public static Calendar getUtcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    }

    public static ArrowType getArrowTypeForCassandraField(CassandraFieldInfo fieldInfo, Calendar calendar) {
        Preconditions.checkNotNull(fieldInfo, "CassandraFieldInfo object cannot be null");
        String timezone;
        if (calendar != null) {
            timezone = calendar.getTimeZone().getID();
        } else {
            timezone = null;
        }

        if (fieldInfo.getDataType().getProtocolCode() == TIME.getProtocolCode()) {
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
        }

        ArrowType type = cassandraToArrowMap.getOrDefault(fieldInfo.getDataType().getProtocolCode(), () -> ArrowType.Null.INSTANCE).get();

        // if no type was found
        if (type.getTypeID().equals(ArrowType.Null.INSTANCE.getTypeID())) {

        }

        return type;
    }

    public static final Map<Integer, Supplier<ArrowType>> cassandraToArrowMap = new HashMap<Integer, Supplier<ArrowType>>() {{
        put(TEXT.getProtocolCode(), () -> new ArrowType.Utf8());
        put(BOOLEAN.getProtocolCode(), () -> new ArrowType.Bool());
        put(TINYINT.getProtocolCode(), () -> new ArrowType.Bool());
        put(SMALLINT.getProtocolCode(), () -> new ArrowType.Int(16, true));
        put(INT.getProtocolCode(), () -> new ArrowType.Int(32, true));
        put(BIGINT.getProtocolCode(), () -> new ArrowType.Int(64, true));
        put(FLOAT.getProtocolCode(), () -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        put(DOUBLE.getProtocolCode(), () -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        put(DATE.getProtocolCode(), () -> new ArrowType.Date(DateUnit.DAY));
        put(TIMEUUID.getProtocolCode(), () -> new ArrowType.Date(DateUnit.MILLISECOND));
        put(TIMESTAMP.getProtocolCode(), () -> new ArrowType.Date(DateUnit.MILLISECOND));
        put(UUID.getProtocolCode(), () -> new ArrowType.FixedSizeBinary(16));
    }};

    public static final DataType getCassandraDataType(String type) {
        switch (type.toLowerCase()) {
            case "text":
                return TEXT;
            case "boolean":
                return BOOLEAN;
            case "tinyint":
                return TINYINT;
            case "smallint":
                return SMALLINT;
            case "int":
                return INT;
            case "bigint":
                return BIGINT;
            case "float":
                return FLOAT;
            case "decimal":
                return DECIMAL;
            case "date":
                return DATE;
            case "timeuuid":
                return TIMEUUID;
            case "timestamp":
                return TIMESTAMP;
            case "uuid":
                return UUID;
            case "asci":
                return ASCII;
            case "blob":
                return BLOB;
            case "counter":
                return COUNTER;
            case "double":
                return DOUBLE;
            case "duration":
                return DURATION;
            case "inet":
                return INET;
            case "time":
                return TIME;
            case "varchar":
                return null;
            case "varint":
                return VARINT;
            case "list":
                return null;
            case "map":
                return null;
            case "set":
                return null;
            default:
                return null;
        }
    }
}