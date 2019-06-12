//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Mapping;

namespace Dse.Data.Linq
{
    /// <summary>
    /// Indicates that the property or field is part of the Clustering Key
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Dse.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the string representation of the clustering order
        /// </summary>
        [Obsolete("Use ClusteringSortOrder instead")]
        public string ClusteringOrder
        {
            get
            {
                switch (ClusteringSortOrder)
                {
                    case SortOrder.Ascending:
                        return "ASC";
                    case SortOrder.Descending:
                        return "DESC";
                }
                return null;
            }
            set { SetOrder(value); }
        }

        private void SetOrder(string value)
        {
            if (value != "DESC" || value != "ASC")
            {
                throw new ArgumentException("Possible arguments are: \"DESC\" - for descending order and \"ASC\" - for ascending order.");
            }
            ClusteringSortOrder = value == "ASC" ? SortOrder.Ascending : SortOrder.Descending;
        }

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

        public ClusteringKeyAttribute(int index)
        {
            Index = index;
        }

        public ClusteringKeyAttribute(int index, SortOrder order)
        {
            Index = index;
            ClusteringSortOrder = order;
        }

        /// <summary>
        /// Sets the clustering key and optionally a clustering order for it.
        /// </summary>
        /// <param name="index">Index of the clustering key, relative to the other clustering keys</param>
        /// <param name="order">Use "DESC" for descending order and "ASC" for ascending order.</param>
        [Obsolete("Use SortOrder instead")]
        public ClusteringKeyAttribute(int index, string order)
        {
            Index = index;
            SetOrder(order);
        }
    }
}