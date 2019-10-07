//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Reflection;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A definition for how to map a POCO.
    /// </summary>
    public interface ITypeDefinition
    {
        /// <summary>
        /// The Type of the POCO.
        /// </summary>
        Type PocoType { get; }

        /// <summary>
        /// The name of the table to map the POCO to.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// The name of the keyspace where the table is defined.
        /// When the keyspace name is not null, the table name for the query generated will be fully qualified (ie: keyspace.tablename)
        /// </summary>
        string KeyspaceName { get; }

        /// <summary>
        /// Whether or not this POCO should only have columns explicitly defined mapped.
        /// </summary>
        bool ExplicitColumns { get; }

        /// <summary>
        /// Gets the partition key columns of the table.
        /// </summary>
        string[] PartitionKeys { get; }

        /// <summary>
        /// Gets the clustering key columns of the table.
        /// </summary>
        Tuple<string, SortOrder>[] ClusteringKeys { get; }

        /// <summary>
        /// Determines if the queries generated using this definition should be case-sensitive
        /// </summary>
        bool CaseSensitive { get; }

        /// <summary>
        /// Determines if the table is declared with COMPACT STORAGE
        /// </summary>
        bool CompactStorage { get; }

        /// <summary>
        /// Determines that all queries generated for this table can be made allowing server side filtering
        /// </summary>
        bool AllowFiltering { get; }

        /// <summary>
        /// Gets a column definition for the given field on the POCO.
        /// </summary>
        IColumnDefinition GetColumnDefinition(FieldInfo field);

        /// <summary>
        /// Gets a column definition for the given property on the POCO.
        /// </summary>
        IColumnDefinition GetColumnDefinition(PropertyInfo property);
    }
}
