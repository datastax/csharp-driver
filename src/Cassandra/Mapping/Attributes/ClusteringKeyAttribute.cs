//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field is part of the Clustering Key
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the clustering order
        /// </summary>
        public SortOrder ClusteringSortOrder { get; set; }

        /// <summary>
        /// Index of the clustering key, relative to the other clustering keys
        /// </summary>
        public int Index { get; set; }

        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; set; }

        public ClusteringKeyAttribute(int index = 0)
        {
            Index = index;
        }

        public ClusteringKeyAttribute(int index, SortOrder order)
        {
            Index = index;
            ClusteringSortOrder = order;
        }
    }
}