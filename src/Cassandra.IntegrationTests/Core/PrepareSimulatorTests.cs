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
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class PrepareSimulatorTests
    {
        private const string Query = "SELECT * FROM ks1.prepare_table1";

        private static object QueryPrime(int delay = 0) => new
        {
            when = new {query = Query},
            then = new
            {
                result = "success",
                delay_in_ms = delay,
                rows = new[] {new {id = Guid.NewGuid()}},
                column_types = new {id = "uuid"},
                ignore_on_prepare = false
            }
        };

        private static readonly object IsBootstrapingPrime = new
        {
            when = new {query = Query},
            then = new
            {
                result = "is_bootstrapping",
                delay_in_ms = 0,
                ignore_on_prepare = false
            }
        };
        
        [Test]
        public void Should_Prepare_On_First_Node()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
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
                Assert.AreEqual(1, node.GetQueries(Query, "PREPARE").Count);
                // Only executed on the first node
                Assert.AreEqual(1, simulacronCluster.GetQueries(Query, "PREPARE").Count);
            }
        }

        [Test]
        public void Should_Prepare_On_All_Nodes_By_Default()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
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
                    Assert.AreEqual(1, node.GetQueries(Query, "PREPARE").Count);   
                }
                // Executed on all nodes
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, "PREPARE").Count);
            }
        }

        [Test]
        public void Should_Reuse_The_Same_Instance()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
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
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
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
                    node.Prime(h == firstHost ? IsBootstrapingPrime : QueryPrime());
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                // Should have been executed in the first node (failed) and in the second one (succeeded)
                Assert.AreEqual(2, simulacronCluster.GetQueries(Query, "PREPARE").Count);
            }
        }

        [Test]
        public void Should_Prepare_On_All_Ignoring_Individual_Failures()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                var secondHost = cluster.AllHosts().Skip(1).First();
                foreach (var h in cluster.AllHosts())
                {
                    var node = simulacronCluster.GetNode(h.Address);
                    node.Prime(h == secondHost ? IsBootstrapingPrime : QueryPrime());
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, "PREPARE").Count);
            }
        }

        [Test]
        public void Should_Failover_When_First_Node_Timeouts()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false))
                                        .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(400))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                var firstHost = cluster.AllHosts().First();
                foreach (var h in cluster.AllHosts())
                {
                    var node = simulacronCluster.GetNode(h.Address);
                    node.Prime(QueryPrime(h == firstHost ? 5000 : 0));
                }
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                // Should have been executed in the first node (timed out) and in the second one (succeeded)
                Assert.AreEqual(2, simulacronCluster.GetQueries(Query, "PREPARE").Count);
            }
        }

        [Test]
        public async Task Should_Reprepare_On_Up_Node()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } ))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(500))
                                        .WithLoadBalancingPolicy(new TestHelper.OrderedLoadBalancingPolicy()).Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime());
                var ps = await session.PrepareAsync(Query).ConfigureAwait(false);
                Assert.NotNull(ps);
                Assert.AreEqual(3, simulacronCluster.GetQueries(Query, "PREPARE").Count);
                var node = simulacronCluster.GetNodes().Skip(1).First();
                // It should have been prepared once on the node we are about to stop
                Assert.AreEqual(1, node.GetQueries(Query, "PREPARE").Count);
                await node.Stop().ConfigureAwait(false);
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().Any(h => !h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(1, cluster.AllHosts().Count(h => !h.IsUp));
                await node.Start().ConfigureAwait(false);
                await TestHelper.WaitUntilAsync(() => cluster.AllHosts().All(h => h.IsUp)).ConfigureAwait(false);
                Assert.AreEqual(0, cluster.AllHosts().Count(h => !h.IsUp));
                TestHelper.WaitUntil(() => node.GetQueries(Query, "PREPARE").Count == 2);
                // It should be prepared 2 times
                Assert.AreEqual(2, node.GetQueries(Query, "PREPARE").Count);
            }
        }
    }
}