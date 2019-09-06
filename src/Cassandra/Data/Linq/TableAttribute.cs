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
    /// Specifies table information for a given class
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Cassandra.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class TableAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the table name in Cassandra
        /// </summary>
        public string Name {get; set; }

        /// <summary>
        /// Determines if the table and column names are defined as case sensitive (default to true).
        /// </summary>
        public bool CaseSensitive { get; set; }

        /// <summary>
        /// Specifies table information for a given class
        /// </summary>
        public TableAttribute()
        {
            //Linq tables are case sensitive by default
            CaseSensitive = true;
        }

        /// <summary>
        /// Specifies table information for a given class
        /// </summary>
        /// <param name="name">Name of the table</param>
        /// <param name="caseSensitive">Determines if the table and column names are defined as case sensitive</param>
        public TableAttribute(string name, bool caseSensitive = true)
        {
            Name = name;
            CaseSensitive = caseSensitive;
        }
    }
}