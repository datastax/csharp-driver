using System;
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
