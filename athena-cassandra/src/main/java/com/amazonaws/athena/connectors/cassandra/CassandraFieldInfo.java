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
package com.amazonaws.athena.connectors.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import org.apache.arrow.util.Preconditions;

// todo - why use 'CqlIdentifier' and not just a string
// todo - differentiate fromCql vs fromInternal.  why use one or the other?

public class CassandraFieldInfo
{

    private final DataType dataType;
    private CqlIdentifier keyspace;
    private CqlIdentifier parent;
    private final CqlIdentifier name;

    public CassandraFieldInfo(String name, String dataType)
    {
        this.dataType = CassandraToArrowUtils.DATA_TYPES_BY_NAME.get(dataType);
        this.name = CqlIdentifier.fromInternal(name);
    }

    public DataType getDataType()
    {
        return this.dataType;
    }

    public CqlIdentifier getName()
    {
        return name;
    }
}
