package com.amazonaws.connectors.athena.cassandra;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class CassandraToArrowTypeConverter {

    /**
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

    public static ArrowType toArrowType(String type, int position, String clustering_order) {



    }


}
