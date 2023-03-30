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