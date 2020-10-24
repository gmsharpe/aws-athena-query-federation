package com.amazonaws.connectors.athena.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.*;
import java.util.function.Supplier;

import static com.datastax.oss.driver.api.core.type.DataTypes.*;

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
            timezone = "UTC";
        }

        if (fieldInfo.getDataType().getProtocolCode() == TIMESTAMP.getProtocolCode()) {
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone);
        }

        ArrowType type = cassandraToArrowMap.getOrDefault(fieldInfo.getDataType().getProtocolCode(), () -> ArrowType.Null.INSTANCE).get();

        // if no type was found
        if (type.getTypeID().equals(ArrowType.Null.INSTANCE.getTypeID())) {

        }

        return type;
    }

    /*
        currently supported ArrowTypes in Athena: SupportedTypes.isSupported(columnType)

     **   BIT(Types.MinorType.BIT),
          DATEMILLI(Types.MinorType.DATEMILLI),
     **   DATEDAY(Types.MinorType.DATEDAY),
     **   TIMESTAMPMILLITZ(Types.MinorType.TIMESTAMPMILLITZ),
     **   FLOAT8(Types.MinorType.FLOAT8),
     **   FLOAT4(Types.MinorType.FLOAT4),
     **   INT(Types.MinorType.INT),
     **   TINYINT(Types.MinorType.TINYINT),
     **   SMALLINT(Types.MinorType.SMALLINT),
     **   BIGINT(Types.MinorType.BIGINT),
     **   VARBINARY(Types.MinorType.VARBINARY),
     **   DECIMAL(Types.MinorType.DECIMAL),
     **   VARCHAR(Types.MinorType.VARCHAR),
     **   STRUCT(Types.MinorType.STRUCT),
     **   LIST(Types.MinorType.LIST);
    */

    public static final Map<Integer, Supplier<ArrowType>> cassandraToArrowMap = new HashMap<Integer, Supplier<ArrowType>>() {{
        put(TEXT.getProtocolCode(), Types.MinorType.VARCHAR::getType); // should this be VARCHAR
        put(ProtocolConstants.DataType.VARCHAR, Types.MinorType.VARCHAR::getType); // should this be VARCHAR
        put(ASCII.getProtocolCode(), Types.MinorType.VARCHAR::getType);
        put(INET.getProtocolCode(), Types.MinorType.VARCHAR::getType);
        put(BOOLEAN.getProtocolCode(), Types.MinorType.BIT::getType);
        put(TINYINT.getProtocolCode(), Types.MinorType.BIT::getType);
        put(SMALLINT.getProtocolCode(), Types.MinorType.SMALLINT::getType);
        put(INT.getProtocolCode(), Types.MinorType.INT::getType);
        put(BIGINT.getProtocolCode(), Types.MinorType.BIGINT::getType);
        put(COUNTER.getProtocolCode(), Types.MinorType.BIGINT::getType);
        put(FLOAT.getProtocolCode(), Types.MinorType.FLOAT4::getType);
        put(DOUBLE.getProtocolCode(), Types.MinorType.FLOAT8::getType);
        put(ProtocolConstants.DataType.VARINT, Types.MinorType.BIGINT::getType); // should this be INT instead?
        put(DATE.getProtocolCode(), Types.MinorType.DATEDAY::getType);
        // nanoseconds since midnight converted to milli.  maybe just convert to _int
        // not supported by AQF, at the moment

        put(TIME.getProtocolCode(),() -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
        // defaults to UTC, if none given prior
        put(TIMESTAMP.getProtocolCode(),() -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")); // could use TIMESTAMPMILLITZ to get DATEMILLI
        put(TIMEUUID.getProtocolCode(), Types.MinorType.VARBINARY::getType); // v1 UUID
        put(UUID.getProtocolCode(), Types.MinorType.VARBINARY::getType); // standard UUID
        put(DECIMAL.getProtocolCode(),() -> new ArrowType.Decimal(0,0));
        put(ProtocolConstants.DataType.BLOB, Types.MinorType.VARBINARY::getType);
        //put(DATE.getProtocolCode(), Types.MinorType.DATEMILLI::getType);
        put(ProtocolConstants.DataType.LIST, Types.MinorType.LIST::getType);
        put(ProtocolConstants.DataType.SET, Types.MinorType.LIST::getType);
        put(ProtocolConstants.DataType.MAP, Types.MinorType.STRUCT::getType);
        put(ProtocolConstants.DataType.TUPLE, Types.MinorType.STRUCT::getType);

    }};


    public static final ImmutableMap<String, DataType> DATA_TYPES_BY_NAME =
            new ImmutableMap.Builder<String, DataType>()
                    .put("ascii", DataTypes.ASCII)
                    .put("bigint", DataTypes.BIGINT)
                    .put("blob", DataTypes.BLOB)
                    .put("boolean", DataTypes.BOOLEAN)
                    .put("counter", DataTypes.COUNTER)
                    .put("decimal", DataTypes.DECIMAL)
                    .put("double", DataTypes.DOUBLE)
                    .put("float", DataTypes.FLOAT)
                    .put("inet", DataTypes.INET)
                    .put("int", DataTypes.INT)
                    .put("text", DataTypes.TEXT)
                    .put("varchar", DataTypes.TEXT)
                    .put("timestamp", DataTypes.TIMESTAMP)
                    .put("date", DataTypes.DATE)
                    .put("time", DataTypes.TIME)
                    .put("uuid", DataTypes.UUID)
                    .put("varint", DataTypes.VARINT)
                    .put("timeuuid", DataTypes.TIMEUUID)
                    .put("tinyint", DataTypes.TINYINT)
                    .put("smallint", DataTypes.SMALLINT)
                    .put("duration", DataTypes.DURATION)
                    .build();

}