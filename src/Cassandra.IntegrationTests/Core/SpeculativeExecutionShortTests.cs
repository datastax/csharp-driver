using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short"), Timeout(60000)]
    public class SpeculativeExecutionShortTests : SharedClusterTest
    {
        private const string QueryLocal = "SELECT key FROM system.local";
        private readonly List<ICluster> _clusters = new List<ICluster>();
        private IPAddress _addressNode1;
        private IPAddress _addressNode2;

        private ISession GetSession(ISpeculativeExecutionPolicy speculativeExecutionPolicy = null, bool warmup = true, ILoadBalancingPolicy lbp = null)
        {
            var cluster = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
                .WithSpeculativeExecutionPolicy(speculativeExecutionPolicy)
                .WithLoadBalancingPolicy(lbp ?? Cassandra.Policies.DefaultLoadBalancingPolicy)
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                .Build();
            _clusters.Add(cluster);
            var session = cluster.Connect();
            if (warmup)
            {
                TestHelper.ParallelInvoke(() => session.Execute(QueryLocal), 10);
            }
            return session;
        }

        /// <summary>
        /// Use 2 nodes
        /// </summary>
        public SpeculativeExecutionShortTests() : base(2)
        {
   
        }

        [TearDown]
        public void TestTearDown()
        {
            //Resume both nodes before each test
            TestCluster.ResumeNode(1);
            TestCluster.ResumeNode(2);
        }

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            _addressNode1 = IPAddress.Parse(TestCluster.ClusterIpPrefix + "1");
            _addressNode2 = IPAddress.Parse(TestCluster.ClusterIpPrefix + "2");
        }

        protected override void TestFixtureTearDown()
        {
            foreach (var c in _clusters)
            {
                c.Dispose();
            }
            base.TestFixtureTearDown();
        }

        [Test]
        public void SpeculativeExecution_Should_Execute_On_Next_Node()
        {
            var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1));
            TestCluster.PauseNode(2);
            Trace.TraceInformation("Node 2 paused");
            TestHelper.ParallelInvoke(() =>
            {
                var rs = session.Execute(new SimpleStatement(QueryLocal).SetIdempotence(true));
                Assert.AreNotEqual(_addressNode2, rs.Info.QueriedHost.Address);
            }, 10);
            TestCluster.ResumeNode(2);
        }

        [Test]
        public void SpeculativeExecution_Should_Not_Execute_On_Next_Node_When_Not_Idempotent()
        {
            var lbp = new OrderedLoadBalancingPolicy(2, 1, 3);
            var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1), true, lbp);
            TestCluster.PauseNode(2);
            var t = session.ExecuteAsync(new SimpleStatement(QueryLocal).SetIdempotence(false));
            Thread.Sleep(200);
            Assert.AreEqual(TaskStatus.WaitingForActivation, t.Status);
            TestCluster.ResumeNode(2);
            Thread.Sleep(200);
            Assert.AreEqual(TaskStatus.RanToCompletion, t.Status);
        }

        private class OrderedLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private readonly int[] _lastOctets;
            private ICluster _cluster;
            private int _hostYielded;

            public int HostYielded
            {
                get { return Thread.VolatileRead(ref _hostYielded); }
            }

            public OrderedLoadBalancingPolicy(params int[] lastOctets)
            {
                _lastOctets = lastOctets;
            }

            public void Initialize(ICluster cluster)
            {
                _cluster = cluster;
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                var hosts = _cluster.AllHosts().ToArray();
                foreach (var lastOctet in _lastOctets)
                {
                    var host = hosts.First(h => TestHelper.GetLastAddressByte(h) == lastOctet);
                    Interlocked.Increment(ref _hostYielded);
                    yield return host;
                }
            }
        }
    }
}
