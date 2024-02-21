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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;
using Cassandra.Tests;

using NUnit.Framework;

#pragma warning disable 618

namespace Cassandra.IntegrationTests.Core
{
    [TestTimeout(60000)]
    public class SpeculativeExecutionShortTests : SimulacronTest
    {
        private const string QueryLocal = "SELECT key FROM system.local";
        private readonly List<ICluster> _clusters = new List<ICluster>();
        private IPAddress _addressNode2;

        private ISession GetSession(
            ISpeculativeExecutionPolicy speculativeExecutionPolicy = null, bool warmup = true,
            ILoadBalancingPolicy lbp = null, PoolingOptions pooling = null)
        {
            var builder = ClusterBuilder()
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
        public SpeculativeExecutionShortTests() : base(false, new SimulacronOptions { Nodes = "2" }, false)
        {
        }
        
        public override void SetUp()
        {
            base.SetUp();
            var contactPoint = TestCluster.GetNode(1).ContactPoint;
            var separatorIndex = contactPoint.IndexOf(":", StringComparison.Ordinal);
            var address = contactPoint.Substring(0, separatorIndex);
            _addressNode2 = IPAddress.Parse(address);
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

            TestCluster.GetNode(1).PrimeFluent(
                b => b.WhenQuery(QueryLocal)
                      .ThenRowsSuccess(new[] { "key" }, r => r.WithRow("local")).WithDelayInMs(10000));

            TestHelper.ParallelInvoke(() =>
            {
                var rs = session.Execute(new SimpleStatement(QueryLocal).SetIdempotence(true));
                Assert.AreNotEqual(_addressNode2, rs.Info.QueriedHost.Address);
            }, 10);
        }

        [Test]
        public async Task SpeculativeExecution_Should_Not_Execute_On_Next_Node_When_Not_Idempotent()
        {
            var lbp = new OrderedLoadBalancingPolicy(
                TestCluster.GetNode(1).Address.ToString(), TestCluster.GetNode(0).Address.ToString());
            var session = GetSession(new ConstantSpeculativeExecutionPolicy(50L, 1), true, lbp);

            TestCluster.GetNode(1).PrimeFluent(
                b => b.WhenQuery(QueryLocal)
                      .ThenRowsSuccess(new[] {"key"}, r => r.WithRow("local")).WithDelayInMs(1000));

            var rs = await session.ExecuteAsync(new SimpleStatement(QueryLocal).SetIdempotence(false)).ConfigureAwait(false);

            // Used the first host in the query plan
            Assert.AreEqual(TestCluster.GetNode(1).IpEndPoint, rs.Info.QueriedHost);
        }

        [Test]
        public void SpeculativeExecution_Should_Not_Schedule_More_Than_Once_On_A_Healthy_Cluster()
        {
            var policy = new LoggedSpeculativeExecutionPolicy(5000);
            var session = GetSession(policy);
            var semaphore = new SemaphoreSlim(10);
            TestHelper.ParallelInvoke(() =>
            {
                semaphore.Wait();
                session.Execute(new SimpleStatement(QueryLocal).SetIdempotence(true));
                semaphore.Release();
            }, 512);
            Assert.AreEqual(0, policy.ScheduledExecutions.Count(e => e > 1), "Scheduled more than once: [" + String.Join(", ", policy.ScheduledExecutions.Where(e => e > 1).Select(x => x.ToString())) + "]");
        }

        [Test]
        public void SpeculativeExecution_LINQSelect_Should_Execute_On_Next_Node_When_Idempotent()
        {
            var policy = new LoggedSpeculativeExecutionPolicy(5000);
            var session = GetSession(policy);
            var numberOfRequests = 64;
            var parallelism = 4;
            var semaphore = new SemaphoreSlim(parallelism);
            TestHelper.ParallelInvoke(() =>
            {
                semaphore.Wait();
                try
                {
                    var table = new Table<AllDataTypesEntity>(session, new MappingConfiguration());
                    table.SetIdempotence(true);
                    table.Execute();
                }
                catch (InvalidTypeException) // we're not priming the query
                {
                }
                finally
                {
                    semaphore.Release();
                }
            }, numberOfRequests);
            Assert.AreEqual(0, policy.ScheduledExecutions.Count(e => e > 1), "Scheduled more than once: [" + String.Join(", ", policy.ScheduledExecutions.Where(e => e > 1).Select(x => x.ToString())) + "]");
            Assert.AreEqual(64, policy.ScheduledExecutions.Count(e => e == 1));
        }

        [Test]
        public void SpeculativeExecution_LINQInsert_Should_Execute_On_Next_Node_When_Idempotent()
        {
            var policy = new LoggedSpeculativeExecutionPolicy(5000);
            var session = GetSession(policy);
            var numberOfRequests = 64;
            var parallelism = 4;
            var semaphore = new SemaphoreSlim(parallelism);
            TestHelper.ParallelInvoke(() =>
            {
                semaphore.Wait();
                try
                {
                    var insert = new Table<AllDataTypesEntity>(session, new MappingConfiguration()).Insert(new AllDataTypesEntity());
                    insert.SetIdempotence(true);
                    insert.Execute();
                }
                catch (InvalidTypeException) // we're not priming the query
                {
                }
                finally
                {
                    semaphore.Release();
                }
            }, numberOfRequests);
            Assert.AreEqual(0, policy.ScheduledExecutions.Count(e => e > 1), "Scheduled more than once: [" + String.Join(", ", policy.ScheduledExecutions.Where(e => e > 1).Select(x => x.ToString())) + "]");
            Assert.AreEqual(64, policy.ScheduledExecutions.Count(e => e == 1));
        }

        private class LoggedSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
        {
            private readonly long _firstDelay;
            private readonly ConcurrentDictionary<ISpeculativeExecutionPlan, int> _scheduledMore = new ConcurrentDictionary<ISpeculativeExecutionPlan, int>();

            public LoggedSpeculativeExecutionPolicy(long firstFirstDelay = 500)
            {
                _firstDelay = firstFirstDelay;
            }

            private void OnScheduled(ISpeculativeExecutionPlan plan)
            {
                _scheduledMore.AddOrUpdate(plan, 1, (k, v) => v + 1);
            }

            public ICollection<int> ScheduledExecutions
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
                    _policy.OnScheduled(this);
                    if (_executions++ < 1)
                    {
                        return _policy._firstDelay;
                    }
                    return 0L;
                }
            }
        }

        private class OrderedLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private readonly string[] _addresses;
            private ICluster _cluster;
            private int _hostYielded;

            public int HostYielded
            {
                get { return Volatile.Read(ref _hostYielded); }
            }

            public OrderedLoadBalancingPolicy(params string[] addresses)
            {
                _addresses = addresses;
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
                foreach (var addr in _addresses)
                {
                    var host = hosts.Single(h => h.Address.Address.ToString() == addr);
                    Interlocked.Increment(ref _hostYielded);
                    yield return host;
                }
            }
        }
    }
}