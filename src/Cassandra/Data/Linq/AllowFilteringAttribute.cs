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

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// The ALLOW FILTERING option allows to explicitly allow queries that require filtering. 
    /// Please note that a query using ALLOW FILTERING may thus have unpredictable performance (for the definition above), i.e. even a query that selects a handful of records may exhibit performance that depends on the total amount of data stored in the cluster.
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Cassandra.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }
}