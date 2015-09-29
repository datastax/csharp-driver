//
//      Copyright (C) 2012-2014 DataStax Inc.
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


using System.Collections.Generic;
using System.Linq;

namespace Cassandra
{
    /// <summary>
    /// Describes a Cassandra table
    /// </summary>
    public class TableMetadata
    {
        /// <summary>
        /// Gets the table name
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// Gets the table columns
        /// </summary>
        public TableColumn[] TableColumns { get; protected set; }

        /// <summary>
        /// Gets a dictionary of columns by name
        /// </summary>
        public IDictionary<string, TableColumn> ColumnsByName { get; protected set; }

        /// <summary>
        /// Gets an array of columns that are part of the partition key in correct order
        /// </summary>
        public TableColumn[] PartitionKeys { get; protected set; }

        /// <summary>
        /// Gets an array of columns that are part of the clustering key in correct order
        /// </summary>
        public TableColumn[] ClusteringKeys { get; protected set; }

        /// <summary>
        /// Gets the table options
        /// </summary>
        public TableOptions Options { get; protected set; }

        /// <summary>
        /// Gets the table indexes by name
        /// </summary>
        public IDictionary<string, IndexMetadata> Indexes { get; protected set; }

        protected TableMetadata()
        {
            //Allow mocks
        }

        internal TableMetadata(string name, IDictionary<string, TableColumn> columns, TableColumn[] partitionKeys, TableColumn[] clusteringKeys, IDictionary<string, IndexMetadata> indexes, TableOptions options)
        {
            Name = name;
            ColumnsByName = columns;
            TableColumns = columns.Values.ToArray();
            PartitionKeys = partitionKeys;
            ClusteringKeys = clusteringKeys;
            Indexes = indexes;
            Options = options;
        }
    }
}
