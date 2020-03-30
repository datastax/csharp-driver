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
    ///  Additional options of the .net Cassandra driver.
    /// </summary>
    public class ClientOptions
    {
        public const int DefaultQueryAbortTimeout = 60000;

        private readonly string _defaultKeyspace;
        private readonly int _queryAbortTimeout = ClientOptions.DefaultQueryAbortTimeout;
        private readonly bool _withoutRowSetBuffering;

        public bool WithoutRowSetBuffering
        {
            get { return _withoutRowSetBuffering; }
        }

        /// <summary>
        /// Gets the query abort timeout for synchronous operations in milliseconds.
        /// </summary>
        public int QueryAbortTimeout
        {
            get { return _queryAbortTimeout; }
        }

        /// <summary>
        /// Gets the keyspace to be used after connecting to the cluster.
        /// </summary>
        public string DefaultKeyspace
        {
            get { return _defaultKeyspace; }
        }

        public ClientOptions()
        {
        }

        public ClientOptions(bool withoutRowSetBuffering, int queryAbortTimeout, string defaultKeyspace)
        {
            _withoutRowSetBuffering = withoutRowSetBuffering;
            _queryAbortTimeout = queryAbortTimeout;
            _defaultKeyspace = defaultKeyspace;
        }
    }
}
