//
//      Copyright (C) 2012 DataStax Inc.
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

using System.Threading;

namespace Cassandra
{
    /// <summary>
    ///  Additional options of the .net Cassandra driver.
    /// </summary>
    public class ClientOptions
    {
        private readonly string _defaultKeyspace;
        private readonly int _queryAbortTimeout = Timeout.Infinite;
        private readonly bool _withoutRowSetBuffering;

        public bool WithoutRowSetBuffering
        {
            get { return _withoutRowSetBuffering; }
        }

        public int QueryAbortTimeout
        {
            get { return _queryAbortTimeout; }
        }

        public string DefaultKeyspace
        {
            get { return _defaultKeyspace; }
        }

        public ClientOptions()
            : this(false, Timeout.Infinite, null)
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

// end namespace