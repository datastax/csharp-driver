using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.SessionManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ClusterTests : TestGlobals
    {
        private SimulacronCluster _testCluster;

        [TearDown]
        public void TestTearDown()
        {
            if (_testCluster != null)
            {
                _testCluster.Remove();
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public Task Cluster_Connect_Should_Initialize_Loadbalancing_With_ControlConnection_Address_Set(bool asyncConnect)
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var lbp = new TestLoadBalancingPolicy();
            var cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithLoadBalancingPolicy(lbp)
                .Build();
            return Connect(cluster, asyncConnect, session =>
            {
                Assert.NotNull(lbp.ControlConnectionHost);
                Assert.AreEqual(_testCluster.InitialContactPoint, lbp.ControlConnectionHost.Address);
            });
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public Task Cluster_Connect_Should_Use_Node2_Address(bool asyncConnect)
        {
            _testCluster = SimulacronCluster.CreateNew(2);
            var nodes = _testCluster.GetNodes().ToArray();
            var contactPoints = nodes.Select(n => n.ContactPoint).ToArray();
            nodes[0].DisableConnectionListener().GetAwaiter().GetResult();
            var lbp = new TestLoadBalancingPolicy();
            var cluster = Cluster.Builder()
                                 .AddContactPoints(contactPoints.Select(s => s.Split(':').First()))
                                 .WithLoadBalancingPolicy(lbp)
                                 .Build();
            return Connect(cluster, asyncConnect, session =>
            {
                Assert.NotNull(lbp.ControlConnectionHost);
                Assert.AreEqual(contactPoints[1], lbp.ControlConnectionHost.Address.ToString());
            });
        }

        /// Tests that MaxProtocolVersion is honored when set
        ///
        /// Cluster_Should_Honor_MaxProtocolVersion_Set tests that the MaxProtocolVersion set when building a cluster is
        /// honored properly by the driver. It first verifies that the default MaxProtocolVersion is the maximum available by
        /// the driver (ProtocolVersion 4 as of driver 3.0.1). It then verifies that a set MaxProtocolVersion is honored when
        /// connecting to a Cassandra cluster. It also verifies that setting an arbitary MaxProtocolVersion is allowed, as the
        /// ProtocolVersion will be negotiated down upon first connection. Finally, it verifies that a MaxProtocolVersion is
        /// not valid.
        ///
        /// @expected_errors ArgumentException When MaxProtocolVersion is set to 0.
        ///
        /// @since 3.0.1
        /// @jira_ticket CSHARP-388
        /// @expected_result MaxProtocolVersion is set and honored upon connection.
        ///
        /// @test_category connection
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Cluster_Should_Honor_MaxProtocolVersion_Set(bool asyncConnect)
        {
            _testCluster = SimulacronCluster.CreateNew(2);

            // Default MaxProtocolVersion
            var clusterDefault = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            Assert.AreEqual(Cluster.MaxProtocolVersion, clusterDefault.Configuration.ProtocolOptions.MaxProtocolVersion);

            // MaxProtocolVersion set
            var clusterMax = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithMaxProtocolVersion(3)
                .Build();
            Assert.AreEqual(3, clusterMax.Configuration.ProtocolOptions.MaxProtocolVersion);
            await Connect(clusterMax, asyncConnect, session =>
            {
                if (CassandraVersion < Version.Parse("2.1"))
                    Assert.AreEqual(2, session.BinaryProtocolVersion);
                else
                    Assert.AreEqual(3, session.BinaryProtocolVersion);
            }).ConfigureAwait(false);
            
            // Arbitary MaxProtocolVersion set, will negotiate down upon connect
            var clusterNegotiate = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithMaxProtocolVersion(10)
                .Build();
            Assert.AreEqual(10, clusterNegotiate.Configuration.ProtocolOptions.MaxProtocolVersion);
            await Connect(clusterNegotiate, asyncConnect, session =>
            {
                Assert.LessOrEqual(4, clusterNegotiate.Configuration.ProtocolOptions.MaxProtocolVersion);
            }).ConfigureAwait(false);

            // ProtocolVersion 0 does not exist
            Assert.Throws<ArgumentException>(
                () => Cluster.Builder().AddContactPoint("127.0.0.1").WithMaxProtocolVersion((byte)0));
        }


        /// <summary>
        /// Validates that the client adds the newly bootstrapped node and eventually queries from it
        /// </summary>
        [Test]
        [Category("realcluster")]
        public async Task Should_Add_And_Query_Newly_Bootstrapped_Node()
        {
            var testCluster = TestClusterManager.CreateNew();
            var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build();
            await Connect(cluster, false, session =>
            {
                Assert.AreEqual(1, cluster.AllHosts().Count);
                testCluster.BootstrapNode(2);
                Trace.TraceInformation("Node bootstrapped");
                var newNodeAddress = testCluster.ClusterIpPrefix + 2;
                var newNodeIpAddress = IPAddress.Parse(newNodeAddress);
                TestHelper.RetryAssert(() =>
                    {
                        Assert.True(TestUtils.IsNodeReachable(newNodeIpAddress));
                        //New node should be part of the metadata
                        Assert.AreEqual(2, cluster.AllHosts().Count);
                    },
                    2000, 
                    30);

                TestHelper.RetryAssert(() =>
                {
                    var host = cluster.AllHosts().FirstOrDefault(h => h.Address.Address.Equals(newNodeIpAddress));
                    Assert.IsNotNull(host);
                    var count = host.Tokens?.Count();
                    Assert.IsTrue(count.HasValue);
                    Assert.IsTrue(count.Value > 0, "Tokens Count: " + count);
                });
                
                TestHelper.RetryAssert(() =>
                    {
                        var rs = session.Execute("SELECT key FROM system.local");
                        Assert.True(rs.Info.QueriedHost.Address.ToString() == newNodeAddress, "Newly bootstrapped node should be queried");
                    },
                    1, 
                    100);
            }).ConfigureAwait(false);
        }

        [Test]
        [Category("realcluster")]
        public async Task Should_Remove_Decommissioned_Node()
        {
            var testCluster = TestClusterManager.CreateNew(2);
            var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint).Build();
            await Connect(cluster, false, session =>
            {
                Assert.AreEqual(2, cluster.AllHosts().Count);
                testCluster.DecommissionNode(2);
                Trace.TraceInformation("Node decommissioned");
                string decommisionedNode = null;
                TestHelper.RetryAssert(() =>
                {
                    decommisionedNode = testCluster.ClusterIpPrefix + 2;
                    Assert.False(TestUtils.IsNodeReachable(IPAddress.Parse(decommisionedNode)));
                    //New node should be part of the metadata
                    Assert.AreEqual(1, cluster.AllHosts().Count);
                }, 100, 100);
                var queried = false;
                for (var i = 0; i < 10; i++)
                {
                    var rs = session.Execute("SELECT key FROM system.local");
                    if (rs.Info.QueriedHost.Address.ToString() == decommisionedNode)
                    {
                        queried = true;
                        break;
                    }
                }
                Assert.False(queried, "Removed node should be queried");
            }).ConfigureAwait(false);
        }

        private class TestLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private ICluster _cluster;
            public Host ControlConnectionHost { get; private set; }

            public void Initialize(ICluster cluster)
            {
                _cluster = cluster;
                ControlConnectionHost = ((IInternalCluster)cluster).GetControlConnection().Host;
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                return _cluster.AllHosts();
            }
        }
    }
}
