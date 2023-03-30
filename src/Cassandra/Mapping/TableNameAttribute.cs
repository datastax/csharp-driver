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
    /// Deprecated (use <see cref="TableAttribute"/>)
    /// Used to specify the table a POCO maps to. Deprecated.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class TableNameAttribute : Attribute
    {
        private readonly string _tableName;

        /// <summary>
        /// The table name.
        /// </summary>
        public string Value
        {
            get { return _tableName; }
        }

        /// <summary>
        /// Specifies the table a POCO maps to.
        /// </summary>
        /// <param name="tableName">The name of the table to map this POCO to.</param>
        public TableNameAttribute(string tableName)
        {
            _tableName = tableName;
        }
    }
}
