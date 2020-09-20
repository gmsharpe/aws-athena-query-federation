package com.amazonaws.athena.connector.lambda.data.writers.holders;

import org.apache.arrow.vector.holders.ValueHolder;

public class NullableFixedSizeBinaryHolder      implements ValueHolder
{
    public int byteWidth;
    public int isSet;
    public byte[] value;
}