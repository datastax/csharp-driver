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

namespace Cassandra.IntegrationTests.TestBase
{
    public class Options : TestGlobals
    {
        public static Options Default = new Options();

        /// <summary>
        /// Cassandra version. For example: 1.2.16 or 2.0.7
        /// </summary>
        public readonly string CASSANDRA_VERSION;

        public readonly string IP_PREFIX;

        public string SSH_HOST;
        public string SSH_PASSWORD;
        public int SSH_PORT;
        public string SSH_USERNAME;

        public bool USE_COMPRESSION;
        public bool USE_LOGGER;
        public bool USE_NOBUFFERING;

        private Options()
        {
            CASSANDRA_VERSION = CassandraVersionStr;

            IP_PREFIX = DefaultIpPrefix;
            SSH_HOST = SSHHost;
            SSH_PORT = SSHPort;
            SSH_USERNAME = SSHUser;
            SSH_PASSWORD = SSHPassword;

            USE_COMPRESSION = UseCompression;
            USE_NOBUFFERING = NoUseBuffering;
            USE_LOGGER = UseLogger;
        }
    }
}
