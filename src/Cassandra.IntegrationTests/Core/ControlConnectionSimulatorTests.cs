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

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class ControlConnectionSimulatorTests : TestGlobals
    {
        [TestCase(ProtocolVersion.V4, "4.0.0", "3.11.6")]
        [TestCase(ProtocolVersion.V4, "4.0.0", "3.0.13")]
        [TestCase(ProtocolVersion.V3, "4.0.0", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "3.11.6", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "3.0.13", "2.1.17")]
        [TestCase(ProtocolVersion.V3, "2.2.11", "2.1.17")]
        [TestCase(ProtocolVersion.V2, "2.2.11", "2.0.17")]
        [TestCase(ProtocolVersion.V2, "2.1.17", "2.0.17")]
        [TestCase(ProtocolVersion.V1, "2.2.11", "1.2.19")]
        [TestCase(ProtocolVersion.V1, "2.1.17", "1.2.19")]
        [TestCase(ProtocolVersion.V1, "2.0.17", "1.2.19")]
        public void Should_Downgrade_To_Protocol_VX_With_Versions(ProtocolVersion version,
                                                                  params string[] cassandraVersions)
        {
            using (var testCluster = SimulacronCluster.CreateNewWithPostBody(GetSimulatorBody(cassandraVersions)))
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                if (version > ProtocolVersion.V2)
                {
                    var session = cluster.Connect();
                    Parallel.For(0, 10, _ => session.Execute("SELECT * FROM system.local"));
                    Assert.AreEqual(version, cluster.InternalRef.GetControlConnection().ProtocolVersion);
                }
                else
                {
                    // Protocol v2 and v1 not supported by simulacron but driver protocol downgrading worked
                    var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                    Assert.That(ex.Errors.Count, Is.EqualTo(2));
                    foreach (var err in ex.Errors.Values)
                    {
                        Assert.IsInstanceOf<UnsupportedProtocolVersionException>(err);
                    }
                }
            }
        }
        
        [TestCase(ProtocolVersion.V5, "4.0.0")]
        [TestCase(ProtocolVersion.V4, "3.11.6", "3.0.11", "2.2.9")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "3.0.11", "2.2.9")]
        // Can't downgrade C* 3.0+ nodes to v1 or v2
        [TestCase(ProtocolVersion.V4, "4.0.0", "3.0.13", "2.0.17")]
        [TestCase(ProtocolVersion.V4, "3.0.13", "1.2.19")]
        [TestCase(ProtocolVersion.V5, "4.0.0", "1.2.19")]
        public void Should_Not_Downgrade_Protocol_Version(ProtocolVersion version, params string[] cassandraVersions)
        {
            using (var testCluster = SimulacronCluster.CreateNewWithPostBody(GetSimulatorBody(cassandraVersions)))
            using (var cluster = ClusterBuilder().WithBetaProtocolVersions().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                Parallel.For(0, 10, _ => session.Execute("SELECT * FROM system.local"));
                Assert.AreEqual(version, cluster.InternalRef.GetControlConnection().ProtocolVersion);
            }
        }

        [Test]
        public async Task Should_Failover_With_Connections_Closing()
        {
            using (var testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "4" }))
            {
                var initialContactPoint = testCluster.InitialContactPoint.Address.GetAddressBytes();
                var port = testCluster.InitialContactPoint.Port;
                var contactPoints = new IPEndPoint[4];
                for (byte i = 0; i < 4; i++)
                {
                    var arr = (byte[])initialContactPoint.Clone();
                    arr[3] += i;
                    contactPoints[i] = new IPEndPoint(new IPAddress(arr), port);
                }
                var builder = ClusterBuilder().AddContactPoints(contactPoints);
                var index = 0;
                await TestHelper.TimesLimit(async () =>
                {
                    var nodeAsDown = -1;
                    var currentIndex = Interlocked.Increment(ref index);
                    switch (currentIndex)
                    {
                        case 11:
                            nodeAsDown = 0;
                            break;

                        case 18:
                            nodeAsDown = 1;
                            break;
                    }

                    if (nodeAsDown >= 0)
                    {
                        await testCluster.GetNodes().Skip(nodeAsDown).First().DisableConnectionListener()
                                         .ConfigureAwait(false);
                        var connections = await testCluster.GetConnectedPortsAsync().ConfigureAwait(false);
                        for (var i = connections.Count - 3; i < connections.Count; i++)
                        {
                            try
                            {
                                await testCluster.DropConnection(connections.Last()).ConfigureAwait(false);
                            }
                            catch
                            {
                                // Connection might be already closed
                            }
                        }
                    }

                    ICluster cluster = null;
                    try
                    {
                        cluster = builder.Build();
                        await cluster.ConnectAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        await (cluster?.ShutdownAsync() ?? TaskHelper.Completed).ConfigureAwait(false);
                    }

                    return 0;
                }, 60, 5).ConfigureAwait(false);
            }
        }

        private static object GetSimulatorBody(IEnumerable<string> cassandraVersions)
        {
            return new
            {
                data_centers = new dynamic[]
                {
                    new
                    {
                        id = 0,
                        name = "dc0",
                        nodes = cassandraVersions.Select((v, index) => new {id = index, cassandra_version = v})
                    }
                }
            };
        }
    }
}