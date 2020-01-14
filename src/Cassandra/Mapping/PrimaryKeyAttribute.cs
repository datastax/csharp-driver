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
using Cassandra.Mapping.Attributes;

namespace Cassandra.Mapping
{
    /// <summary>
    /// DEPRECATED (use <see cref="PartitionKeyAttribute"/> and <see cref="ClusteringKeyAttribute"/>).
    /// An attribute used to indicate the primary key column names for the table a POCO is mapped to.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class PrimaryKeyAttribute : Attribute
    {
        private readonly string[] _columnNames;

        /// <summary>
        /// The column names of the primary key columns for the table.
        /// </summary>
        public string[] ColumnNames
        {
            get { return _columnNames; }
        }

        /// <summary>
        /// Specify the primary key column names (in order) for the table.
        /// </summary>
        /// <param name="columnNames">The column names for the table's primary key.</param>
        public PrimaryKeyAttribute(params string[] columnNames)
        {
            if (columnNames == null) throw new ArgumentNullException("columnNames");
            if (columnNames.Length < 1) throw new ArgumentOutOfRangeException("columnNames", "You must specify at least one primary key column name.");

            _columnNames = columnNames;
        }
    }
}
