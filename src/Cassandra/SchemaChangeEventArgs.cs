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

namespace Cassandra
{
    internal class SchemaChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            Created,
            Updated,
            Dropped
        };

        /// <summary>
        /// The keyspace affected
        /// </summary>
        public string Keyspace { get; set; }
        /// <summary>
        /// The table affected
        /// </summary>
        public string Table { get; set; }
        public Reason What { get; set; }
        /// <summary>
        /// The custom type affected
        /// </summary>
        public string Type { get; set; }
    }
}