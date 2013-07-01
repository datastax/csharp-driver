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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    public class Options
    {

        public static Options Default = new Options();

        public readonly string IP_PREFIX;

        public readonly string CASSANDRA_VERSION;

        public string SSH_HOST;
        public int SSH_PORT;
        public string SSH_USERNAME;
        public string SSH_PASSWORD;

        private Options()
        {
            IP_PREFIX = MyTest.MyTestOptions.Default.IpPrefix;
            SSH_HOST = MyTest.MyTestOptions.Default.SSHHost;
            SSH_PORT = MyTest.MyTestOptions.Default.SSHPort;
            SSH_USERNAME = MyTest.MyTestOptions.Default.SSHUser;
            SSH_PASSWORD = MyTest.MyTestOptions.Default.SSHPassword;
            CASSANDRA_VERSION = "-v " + MyTest.MyTestOptions.Default.CassandraVersion;

            USE_COMPRESSION = MyTest.MyTestOptions.Default.UseCompression;
            USE_NOBUFFERING = MyTest.MyTestOptions.Default.NoUseBuffering;
            USE_LOGGER = MyTest.MyTestOptions.Default.UseLogger;
        }

        public bool USE_COMPRESSION;
        public bool USE_NOBUFFERING;
        public bool USE_LOGGER;

    }
}
