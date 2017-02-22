//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement
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
