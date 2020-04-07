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
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// A representation of a secondary index in Cassandra
    /// </summary>
    public class IndexMetadata
    {
        /// <summary>
        /// Describes the possible kinds of indexes
        /// </summary>
        public enum IndexKind
        {
            Keys,
            Custom,
            Composites
        }

        private static IndexKind GetKindByName(string name)
        {
            if (!Enum.TryParse(name, true, out IndexKind result))
            {
                return IndexKind.Custom;
            }
            return result;
        }

        /// <summary>
        /// Gets the index name
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the index target
        /// </summary>
        public string Target { get; private set; }

        /// <summary>
        /// Gets the index kind
        /// </summary>
        public IndexKind Kind { get; private set; }

        /// <summary>
        /// Gets index options
        /// </summary>
        public IDictionary<string, string> Options { get; private set; }

        public IndexMetadata(string name, string target, IndexKind kind, IDictionary<string, string> options)
        {
            Name = name;
            Target = target;
            Kind = kind;
            Options = options;
        }

        /// <summary>
        /// From legacy columns
        /// </summary>
        internal static IndexMetadata FromTableColumn(TableColumn c)
        {
            //using obsolete properties
            #pragma warning disable 618
            string target = null;
            if (c.SecondaryIndexOptions.ContainsKey("index_keys"))
            {
                target = string.Format("keys({0})", c.Name);
            }
            else if (c.SecondaryIndexOptions.ContainsKey("index_keys_and_values"))
            {
                target = string.Format("entries({0})", c.Name);
            }
            else if (c.TypeCode == ColumnTypeCode.List || c.TypeCode == ColumnTypeCode.Set || c.TypeCode == ColumnTypeCode.Map)
            {
                target = string.Format("values({0})", c.Name);
            }
            return new IndexMetadata(c.SecondaryIndexName, target, GetKindByName(c.SecondaryIndexType), c.SecondaryIndexOptions);
            #pragma warning restore 618
        }

        /// <summary>
        /// From a row in the 'system_schema.indexes' table
        /// </summary>
        internal static IndexMetadata FromRow(IRow row)
        {
            var options = row.GetValue<IDictionary<string, string>>("options");
            return new IndexMetadata(row.GetValue<string>("index_name"), options["target"], GetKindByName(row.GetValue<string>("kind")), options);
        }
    }
}
