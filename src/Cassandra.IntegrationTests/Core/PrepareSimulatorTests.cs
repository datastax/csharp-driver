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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class PrepareSimulatorTests
    {
        private const string Keyspace = "ks1";
        private static readonly string Query = $"SELECT * FROM {PrepareSimulatorTests.Keyspace}.prepare_table1";
        
        private static readonly string QueryWithoutKeyspace = $"SELECT * FROM prepare_table1";

        private static IPrimeRequest QueryPrime(int delay = 0)
        {
            return SimulacronBase
                   .PrimeBuilder()
                   .WhenQuery(PrepareSimulatorTests.Query)
                   .ThenRowsSuccess(new[] { ("id", DataType.Uuid) }, rows => rows.WithRow(Guid.NewGuid()))
                   .WithDelayInMs(delay)
                   .BuildRequest();
        }
        
        private static IPrimeRequest QueryWithoutKeyspacePrime(int delay = 0)
        {
            return SimulacronBase
                   .PrimeBuilder()
                   .WhenQuery(PrepareSimulatorTests.QueryWithoutKeyspace)
                   .ThenRowsSuccess(new[] { ("id", DataType.Uuid) }, rows => rows.WithRow(Guid.NewGuid()))
                   .WithDelayInMs(delay)
                   .BuildRequest();
        }

        private static IPrimeRequest IsBootstrappingPrime =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(PrepareSimulatorTests.Query)
                .ThenIsBootstrapping()
                .BuildRequest();

        [Test]
        public void Should_Prepare_On_First_Node()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime());
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                Assert.AreEqual(Query, ps.Cql);
                var firstRow = session.Execute(ps.Bind()).FirstOrDefault();
                Assert.NotNull(firstRow);
                var node = simulacronCluster.GetNode(cluster.AllHosts().First().Address);
                // Executed on first node
                Assert.AreEqual(1, node.GetQueries(Query, QueryType.Prepare).Count);
                // Only executed on the first node
                Assert.AreEqual(1, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
            }
        }

        [Test]
        public void Should_Prepare_On_All_Nodes_By_Default()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime());
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                // Executed on each node
                foreach (var node in simulacronCluster.DataCenters[0].Nodes)
                {
                    Assert.AreEqual(1, node.GetQueries(Query, QueryType.Prepare).Count);
                }
                // Executed on all nodes
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
            }
        }

        [Test]
        public void Should_Reuse_The_Same_Instance()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime());
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                Assert.AreSame(ps, session.Prepare(Query));
                Assert.AreNotSame(ps, session.Prepare("SELECT * FROM system.local"));
            }
        }

        [Test]
        public void Should_Failover_When_First_Node_Fails()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                var firstHost = cluster.AllHosts().First();
                foreach (var h in cluster.AllHosts())
                {
                    var node = simulacronCluster.GetNode(h.Address);
                    node.Prime(h == firstHost ? PrepareSimulatorTests.IsBootstrappingPrime : QueryPrime());
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                // Should have been executed in the first node (failed) and in the second one (succeeded)
                Assert.AreEqual(2, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
            }
        }

        [Test]
        public void Should_Prepare_On_All_Ignoring_Individual_Failures()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                var secondHost = cluster.AllHosts().Skip(1).First();
                foreach (var h in cluster.AllHosts())
                {
                    var node = simulacronCluster.GetNode(h.Address);
                    node.Prime(h == secondHost ? PrepareSimulatorTests.IsBootstrappingPrime : QueryPrime());
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
            }
        }

        [Test]
        public void Should_Failover_When_First_Node_Timeouts()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false))
                                        .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(1000))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                var firstHost = cluster.AllHosts().First();
                foreach (var h in cluster.AllHosts())
                {
                    var node = simulacronCluster.GetNode(h.Address);
                    node.Prime(QueryPrime(h == firstHost ? 10000 : 0));
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                // Should have been executed in the first node (timed out) and in the second one (succeeded)
                Assert.AreEqual(2, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
            }
        }

        [Test]
        public async Task Should_Reprepare_On_Up_Node()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(500))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime());
                var ps = await session.PrepareAsync(Query).ConfigureAwait(false);
                Assert.NotNull(ps);
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, QueryType.Prepare).Count);
                var node = simulacronCluster.GetNodes().Skip(1).First();
                // It should have been prepared once on the node we are about to stop
                Assert.AreEqual(1, node.GetQueries(Query, QueryType.Prepare).Count);
                await node.Stop().ConfigureAwait(false);
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().Any(h => !h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(1, cluster.AllHosts().Count(h => !h.IsUp));
                await node.Start().ConfigureAwait(false);
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().All(h => h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(0, cluster.AllHosts().Count(h => !h.IsUp));
                TestHelper.WaitUntil(() => node.GetQueries(Query, QueryType.Prepare).Count == 2);
                // It should be prepared 2 times
                Assert.AreEqual(2, node.GetQueries(Query, QueryType.Prepare).Count);
            }
        }
        
        [Test]
        public async Task Should_ReprepareOnUpNodeAfterSetKeyspace_With_SessionKeyspace()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" }))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(500))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect(PrepareSimulatorTests.Keyspace);

                simulacronCluster.Prime(PrepareSimulatorTests.QueryWithoutKeyspacePrime());
                var ps = await session.PrepareAsync(PrepareSimulatorTests.QueryWithoutKeyspace).ConfigureAwait(false);
                Assert.NotNull(ps);
                ps = await session.PrepareAsync(PrepareSimulatorTests.Query).ConfigureAwait(false);
                Assert.NotNull(ps);

                foreach (var simNode in simulacronCluster.DataCenters.First().Nodes)
                {
                    Assert.AreEqual(1, simNode.GetQueries($"USE \"{PrepareSimulatorTests.Keyspace}\"").Count);
                    Assert.AreEqual(1, simNode.GetQueries(PrepareSimulatorTests.QueryWithoutKeyspace, QueryType.Prepare).Count);
                    Assert.AreEqual(1, simNode.GetQueries(PrepareSimulatorTests.Query, QueryType.Prepare).Count);
                }

                var node = simulacronCluster.GetNodes().Skip(1).First();
                await node.Stop().ConfigureAwait(false);
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().Any(h => !h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(1, cluster.AllHosts().Count(h => !h.IsUp));
                
                // still only 1 USE and Prepare requests
                Assert.AreEqual(1, node.GetQueries($"USE \"{PrepareSimulatorTests.Keyspace}\"").Count);
                Assert.AreEqual(1, node.GetQueries(PrepareSimulatorTests.QueryWithoutKeyspace, QueryType.Prepare).Count);
                Assert.AreEqual(1, node.GetQueries(PrepareSimulatorTests.QueryWithoutKeyspace, QueryType.Prepare).Count);

                // restart node
                await node.Start().ConfigureAwait(false);

                // wait until node is up
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().All(h => h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(0, cluster.AllHosts().Count(h => !h.IsUp));

                // wait until driver reprepares the statement
                TestHelper.WaitUntil(() => 
                    node.GetQueries(PrepareSimulatorTests.QueryWithoutKeyspace, QueryType.Prepare).Count == 2
                    && node.GetQueries(PrepareSimulatorTests.Query, QueryType.Prepare).Count == 2);
                
                // It should be prepared 2 times
                Assert.AreEqual(2, node.GetQueries(PrepareSimulatorTests.QueryWithoutKeyspace, QueryType.Prepare).Count);
                Assert.AreEqual(2, node.GetQueries(PrepareSimulatorTests.Query, QueryType.Prepare).Count);
                Assert.AreEqual(2, node.GetQueries($"USE \"{PrepareSimulatorTests.Keyspace}\"").Count);

                // Assert that USE requests are sent **before** PREPARE requests
                var relevantQueries = node.GetQueries(null, null).Where(log =>
                    (log.Query == $"USE \"{PrepareSimulatorTests.Keyspace}\"" && log.Type == QueryType.Query)
                    || (log.Query == PrepareSimulatorTests.QueryWithoutKeyspace && log.Type == QueryType.Prepare)
                    || (log.Query == PrepareSimulatorTests.Query && log.Type == QueryType.Prepare)).ToList();
                Assert.AreEqual(6, relevantQueries.Count);
                Assert.AreEqual($"USE \"{PrepareSimulatorTests.Keyspace}\"", relevantQueries[0].Query);
                Assert.AreEqual(PrepareSimulatorTests.QueryWithoutKeyspace, relevantQueries[1].Query);
                Assert.AreEqual(PrepareSimulatorTests.Query, relevantQueries[2].Query);
                Assert.AreEqual($"USE \"{PrepareSimulatorTests.Keyspace}\"", relevantQueries[3].Query);
                CollectionAssert.AreEquivalent(
                    new []
                    {
                        PrepareSimulatorTests.QueryWithoutKeyspace,
                        PrepareSimulatorTests.Query
                    }, 
                    relevantQueries.Skip(4).Select(q => q.Query));
            }
        }
    }
}