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
using System;

namespace Cassandra.IntegrationTests
{
    public class Options
    {
        public static Options Default = new Options();

        /// <summary>
        /// Cassandra version. For example: 1.2.16 or 2.0.7
        /// </summary>
        public readonly string CASSANDRA_VERSION;

        public Version CassandraVersion
        {
            get
            {
                int mayor = 0, minor = 0, build = 0;
                if (this.CASSANDRA_VERSION != null)
                {
                    var versionParts = this.CASSANDRA_VERSION.Split('.');
                    if (versionParts.Length >= 2)
                    {
                        mayor = Convert.ToInt32(versionParts[0]);
                        minor = Convert.ToInt32(versionParts[1]);
                        if (versionParts.Length == 3)
                        {
                            build = Convert.ToInt32(versionParts[2]);
                        }
                    }
                }
                return new Version(mayor, minor, build);
            }
        }

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
            IP_PREFIX = MyTestOptions.Default.IpPrefix;
            SSH_HOST = MyTestOptions.Default.SSHHost;
            SSH_PORT = MyTestOptions.Default.SSHPort;
            SSH_USERNAME = MyTestOptions.Default.SSHUser;
            SSH_PASSWORD = MyTestOptions.Default.SSHPassword;
            CASSANDRA_VERSION = MyTestOptions.Default.CassandraVersion;

            USE_COMPRESSION = MyTestOptions.Default.UseCompression;
            USE_NOBUFFERING = MyTestOptions.Default.NoUseBuffering;
            USE_LOGGER = MyTestOptions.Default.UseLogger;
        }
    }
}