using System;
using System.Collections.Concurrent;
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
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short"), TestTimeout(60000)]
    public class SpeculativeExecutionShortTests : SharedClusterTest
    {
        private const string QueryLocal = "SELECT key FROM system.local";
        private readonly List<ICluster> _clusters = new List<ICluster>();
        private IPAddress _addressNode2;

        private ISession GetSession(
            ISpeculativeExecutionPolicy speculativeExecutionPolicy = null, bool warmup = true, 
            ILoadBalancingPolicy lbp = null, PoolingOptions pooling = null)
        {
            var builder = Cluster.Builder()
                .AddContactPoint(TestCluster.InitialContactPoint)
                .WithSpeculativeExecutionPolicy(speculativeExecutionPolicy)
                .WithLoadBalancingPolicy(lbp ?? Cassandra.Policies.DefaultLoadBalancingPolicy)
                .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance)
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0));
            if (pooling != null)
            {
                builder.WithPoolingOptions(pooling);
            }
            var cluster = builder.Build();
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

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _addressNode2 = IPAddress.Parse(TestCluster.ClusterIpPrefix + "2");
        }

        public override void OneTimeTearDown()
        {
            foreach (var c in _clusters)
            {
                c.Dispose();
            }
            base.OneTimeTearDown();
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
        public async Task SpeculativeExecution_Should_Not_Execute_On_Next_Node_When_Not_Idempotent()
        {
            var lbp = new OrderedLoadBalancingPolicy(2, 1);
            var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1), true, lbp);
            TestCluster.PauseNode(2);
            var t = session.ExecuteAsync(new SimpleStatement(QueryLocal).SetIdempotence(false));
            await Task.Delay(200).ConfigureAwait(false);
            Assert.AreEqual(TaskStatus.WaitingForActivation, t.Status);

            TestCluster.ResumeNode(2);
            var rs = await t.ConfigureAwait(false);

            // Used the first host in the query plan
            Assert.AreEqual(2, TestHelper.GetLastAddressByte(rs.Info.QueriedHost));
        }

        [Test]
        public void SpeculativeExecution_Should_Not_Schedule_More_Than_Once_On_A_Healthy_Cluster()
        {
            var policy = new LoggedSpeculativeExecutionPolicy();
            var session = GetSession(policy);
            var semaphore = new SemaphoreSlim(10);
            TestHelper.ParallelInvoke(() =>
            {
                semaphore.Wait();
                session.Execute(new SimpleStatement(QueryLocal).SetIdempotence(true));
                semaphore.Release();
            }, 512);
            Assert.AreEqual(0, policy.ScheduledMoreThanOnce.Count, "Scheduled more than once: [" + String.Join(", ", policy.ScheduledMoreThanOnce.Select(x => x.ToString())) + "]");
        }

        private class LoggedSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
        {
            private readonly ConcurrentDictionary<ISpeculativeExecutionPlan, int> _scheduledMore = new ConcurrentDictionary<ISpeculativeExecutionPlan, int>();

            private void SetScheduledMore(ISpeculativeExecutionPlan plan, int executions)
            {
                _scheduledMore.AddOrUpdate(plan, executions, (k, v) => executions);
            }

            public ICollection<int> ScheduledMoreThanOnce
            {
                get { return _scheduledMore.Values; }
            }

            public void Dispose()
            {
                
            }

            public void Initialize(ICluster cluster)
            {

            }

            public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
            {
                return new LoggedSpeculativeExecutionPlan(this);
            }

            private class LoggedSpeculativeExecutionPlan : ISpeculativeExecutionPlan
            {
                private readonly LoggedSpeculativeExecutionPolicy _policy;
                private int _executions;
                public LoggedSpeculativeExecutionPlan(LoggedSpeculativeExecutionPolicy policy)
                {
                    _policy = policy;
                }

                public long NextExecution(Host lastQueried)
                {
                    if (_executions++ < 1)
                    {
                        return 500L;
                    }
                    _policy.SetScheduledMore(this, _executions);
                    return 0L;
                }
            }
        }

        private class OrderedLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private readonly int[] _lastOctets;
            private ICluster _cluster;
            private int _hostYielded;

            public int HostYielded
            {
                get { return Volatile.Read(ref _hostYielded); }
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
