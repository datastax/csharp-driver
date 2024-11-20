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

namespace Cassandra
{
    /// <summary>
    /// A PREPARE request.
    /// </summary>
    public sealed class PrepareRequest
    {
        internal PrepareRequest(string cql, string keyspace)
        {
            Query = cql;
            Keyspace = keyspace;
        }

        /// <summary>
        /// Returns the keyspace this PREPARE operates on. Can be null.
        /// </summary>
        public string Keyspace { get; }

        /// <summary>
        /// Returns the query string of this PREPARE request.
        /// </summary>
        public string Query { get; }
    }
}
