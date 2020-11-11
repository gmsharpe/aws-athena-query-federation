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
package com.amazonaws.connectors.athena.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import org.apache.arrow.util.Preconditions;

public class CassandraFieldInfo
{

    private final DataType dataType;
    private CqlIdentifier keyspace;
    private CqlIdentifier parent;
    private CqlIdentifier name;

    public CassandraFieldInfo(String dataType)
    {
        this.dataType = CassandraToArrowUtils.DATA_TYPES_BY_NAME.get(dataType);
    }

    public CassandraFieldInfo(DataType dataType)
    {
        // Preconditions.checkArgument(dataType != 3 && dataType != 2, "DECIMAL and NUMERIC types require a precision and scale; please use another constructor.");
        this.dataType = dataType;
    }

    public CassandraFieldInfo(int protocolCode)
    {
        // Preconditions.checkArgument(dataType != 3 && dataType != 2, "DECIMAL and NUMERIC types require a precision and scale; please use another constructor.");
        this.dataType = new PrimitiveType(protocolCode);
    }

    public CassandraFieldInfo(DataType dataType, int precision, int scale)
    {
        this.dataType = dataType;
    }

    public CassandraFieldInfo(ColumnMetadata columnMetadata, int column)
    {
        Preconditions.checkNotNull(columnMetadata, "ColumnMetadata cannot be null.");
        Preconditions.checkArgument(column > 0, "ResultSetMetaData columns have indices starting at 1.");
        this.dataType = columnMetadata.getType();
    }

    public CassandraFieldInfo(ColumnDefinitions columnDefinitions, int column)
    {
        Preconditions.checkNotNull(columnDefinitions, "ColumnDefinitions cannot be null.");
        Preconditions.checkArgument(column > 0, "ColumnDefinitions columns have indices starting at 1.");
        Preconditions.checkArgument(column <= columnDefinitions.size(),
                                    "The index must be within the number of columns (1 to %s, inclusive)",
                                    columnDefinitions.size());
        this.dataType = columnDefinitions.get(column).getType();
    }

    public DataType getDataType()
    {
        return this.dataType;
    }
}
