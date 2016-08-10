using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short"), TestFixture]
    public class ClusterTests : TestGlobals
    {
        private ITestCluster _testCluster;

        [TearDown]
        public void TestTearDown()
        {
            if (_testCluster != null)
            {
                _testCluster.Remove();
            }
        }

        [Test]
        public void Cluster_Connect_Should_Initialize_Loadbalancing_With_ControlConnection_Address_Set()
        {
            _testCluster = TestClusterManager.CreateNew(2);
            var lbp = new TestLoadBalancingPolicy();
            var builder = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithLoadBalancingPolicy(lbp);
            using (var cluster = builder.Build())
            {
                cluster.Connect();
                Assert.NotNull(lbp.ControlConnectionHost);
                Assert.AreEqual(IPAddress.Parse(_testCluster.InitialContactPoint), 
                    lbp.ControlConnectionHost.Address.Address);
            }
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
        public void Cluster_Should_Honor_MaxProtocolVersion_Set()
        {
            _testCluster = TestClusterManager.CreateNew(2);

            // Default MaxProtocolVersion
            var cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build();
            Assert.AreEqual(Cluster.MaxProtocolVersion, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);

            // MaxProtocolVersion set
            cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithMaxProtocolVersion(3)
                .Build();
            Assert.AreEqual(3, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);

            var session = cluster.Connect();
            if (CassandraVersion < Version.Parse("2.1"))
                Assert.AreEqual(2, session.BinaryProtocolVersion);
            else
                Assert.AreEqual(3, session.BinaryProtocolVersion);
            cluster.Shutdown();

            // Arbitary MaxProtocolVersion set, will negotiate down upon connect
            cluster = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithMaxProtocolVersion(10)
                .Build();
            Assert.AreEqual(10, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);
            session = cluster.Connect();
            Assert.LessOrEqual(4, cluster.Configuration.ProtocolOptions.MaxProtocolVersion);
            cluster.Shutdown();

            // ProtocolVersion 0 does not exist
            Assert.Throws<ArgumentException>(() => Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).WithMaxProtocolVersion(0));
        }


        /// <summary>
        /// Validates that the client adds the newly bootstrapped node and eventually queries from it
        /// </summary>
        [Test]
        public void Should_Add_And_Query_Newly_Bootstrapped_Node()
        {
            _testCluster = TestClusterManager.CreateNew();
            using (var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                Assert.AreEqual(1, cluster.AllHosts().Count);
                _testCluster.BootstrapNode(2);
                var queried = false;
                Trace.TraceInformation("Node bootstrapped");
                Thread.Sleep(10000);
                var newNodeAddress = _testCluster.ClusterIpPrefix + 2;
                Assert.True(TestUtils.IsNodeReachable(IPAddress.Parse(newNodeAddress)));
                //New node should be part of the metadata
                Assert.AreEqual(2, cluster.AllHosts().Count);
                for (var i = 0; i < 10; i++)
                {
                    var rs = session.Execute("SELECT key FROM system.local");
                    if (rs.Info.QueriedHost.Address.ToString() == newNodeAddress)
                    {
                        queried = true;
                        break;
                    }
                }
                Assert.True(queried, "Newly bootstrapped node should be queried");
            }
        }

        [Test]
        public void Should_Remove_Decommissioned_Node()
        {
            _testCluster = TestClusterManager.CreateNew(2);
            using (var cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                Assert.AreEqual(2, cluster.AllHosts().Count);
                _testCluster.DecommissionNode(2);
                Trace.TraceInformation("Node decommissioned");
                Thread.Sleep(10000);
                var decommisionedNode = _testCluster.ClusterIpPrefix + 2;
                Assert.False(TestUtils.IsNodeReachable(IPAddress.Parse(decommisionedNode)));
                //New node should be part of the metadata
                Assert.AreEqual(1, cluster.AllHosts().Count);
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
            }
        }

        private class TestLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private ICluster _cluster;
            public Host ControlConnectionHost { get; private set; }

            public void Initialize(ICluster cluster)
            {
                _cluster = cluster;
                ControlConnectionHost = ((Cluster)cluster).GetControlConnection().Host;
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
