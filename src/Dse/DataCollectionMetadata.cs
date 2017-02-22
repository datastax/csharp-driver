//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Describes a table or materialized view in Cassandra
    /// </summary>
    public abstract class DataCollectionMetadata
    {
        /// <summary>
        /// Specifies sort order of the clustering keys
        /// </summary>
        public enum SortOrder : sbyte
        {
            Ascending = 1,
            Descending = -1
        }

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
        /// Gets an array of pairs of columns and sort order that are part of the clustering key
        /// </summary>
        public Tuple<TableColumn, SortOrder>[] ClusteringKeys { get; protected set; }

        /// <summary>
        /// Gets the table options
        /// </summary>
        public TableOptions Options { get; protected set; }

        protected DataCollectionMetadata()
        {
   
        }

        internal void SetValues(IDictionary<string, TableColumn> columns, TableColumn[] partitionKeys, Tuple<TableColumn, SortOrder>[] clusteringKeys, TableOptions options)
        {
            ColumnsByName = columns;
            TableColumns = columns.Values.ToArray();
            PartitionKeys = partitionKeys;
            ClusteringKeys = clusteringKeys;
            Options = options;
        }
    }
}
