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
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.TestBase
{
    [TestFixture]
    public abstract class TestGlobals
    {
        public const int DefaultCassandraPort = 9042;
        public const int DefaultMaxClusterCreateRetries = 2;
        public const string DefaultLocalIpPrefix = "127.0.0.";
        public const string DefaultInitialContactPoint = TestGlobals.DefaultLocalIpPrefix + "1";

        /// <summary>
        /// Determines if we are running on AppVeyor.
        /// </summary>
        protected static bool IsAppVeyor => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APPVEYOR"));

        /// <summary>
        /// Gets the latest protocol version depending on the Cassandra Version running the tests
        /// </summary>
        public ProtocolVersion GetProtocolVersion()
        {
            var cassandraVersion = TestClusterManager.CassandraVersion;
            var protocolVersion = ProtocolVersion.V1;
            if (cassandraVersion >= Version.Parse("2.2"))
            {
                protocolVersion = ProtocolVersion.V4;
            }
            else if (cassandraVersion >= Version.Parse("2.1"))
            {
                protocolVersion = ProtocolVersion.V3;
            }
            else if (cassandraVersion > Version.Parse("2.0"))
            {
                protocolVersion = ProtocolVersion.V2;
            }
            return protocolVersion;
        }

        internal ISerializer GetSerializer()
        {
            return new SerializerManager(GetProtocolVersion()).GetCurrentSerializer();
        }

        public static async Task ConnectAndDispose(Cluster cluster, bool asyncConnect, Action<ISession> action)
        {
            if (asyncConnect)
            {
                try
                {
                    var session = await cluster.ConnectAsync().ConfigureAwait(false);
                    action(session);
                }
                finally
                {
                    var shutdownAsync = cluster?.ShutdownAsync();
                    if (shutdownAsync != null) await shutdownAsync.ConfigureAwait(false);
                }
            }
            else
            {
                using (cluster)
                {
                    var session = cluster.Connect();
                    action(session);
                }
            }
        }

        protected Builder ClusterBuilder()
        {
            return TestUtils.NewBuilder();
        }
    }
}