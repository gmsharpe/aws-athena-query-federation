package com.amazonaws.athena.connector.lambda.data.writers.extractors;

import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableFixedSizeBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;

/**
 * Used to extract a VarBinary value from the context object. This interface enables the use of a pseudo-code generator
 * for RowWriter which reduces object and branching overhead when translating from your source system to Apache
 * Arrow.
 * <p>
 * For example of how to use this, see ExampleRecordHandler in athena-federation-sdk.
 */
public interface FixedSizeBinaryExtractor  extends Extractor
{
    /**
     * Used to extract a value from the context.
     *
     * @param context This is the object you provided to GeneratorRowWriter and is frequently the handle to the source
     * system row/query from which you need to extract a value.
     * @param dst The 'Holder' that you should write your value to and optionally set the isSet flag to > 0 for non-null
     * or 0 for null.
     * @throws Exception internal exception.
     */
    void extract(Object context, NullableFixedSizeBinaryHolder dst) throws Exception;
}
