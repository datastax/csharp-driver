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

using System;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public string ClusteringOrder = null;
        public int Index = -1;
        public string Name;

        public ClusteringKeyAttribute(int index)
        {
            Index = index;
        }

        /// <summary>
        /// Sets the clustering key and optionally a clustering order for it.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="order">Use "DESC" for descending order and "ASC" for ascending order.</param>
        public ClusteringKeyAttribute(int index, string order)
        {
            Index = index;

            if (order == "DESC" || order == "ASC")
                ClusteringOrder = order;
            else
                throw new ArgumentException("Possible arguments are: \"DESC\" - for descending order and \"ASC\" - for ascending order.");
        }
    }
}