using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
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
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
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
