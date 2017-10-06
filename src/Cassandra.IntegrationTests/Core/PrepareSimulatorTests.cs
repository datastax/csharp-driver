//
//      Copyright (C) 2017 DataStax Inc.
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
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class PrepareSimulatorTests
    {
        private const string Query = "SELECT * FROM ks1.prepare_table1";
        
        private static readonly object QueryPrime = new
        {
            when = new { query = Query },
            then = new
            {
                result = "success", 
                delay_in_ms = 0,
                rows = new [] { new { id = Guid.NewGuid() } },
                column_types = new { id = "uuid" }
            }
        };
        
        [Test]
        public void Should_Prepare_On_First_Node()
        {
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } );
            var builder = Cluster.Builder()
                                 .AddContactPoint(simulacronCluster.InitialContactPoint)
                                 .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false))
                                 .WithLoadBalancingPolicy(new OrderedLoadBalancingPolicy());
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime);
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
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
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } );
            var builder = Cluster.Builder()
                                 .AddContactPoint(simulacronCluster.InitialContactPoint)
                                 .WithLoadBalancingPolicy(new OrderedLoadBalancingPolicy());
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime);
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
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } );
            var builder = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(QueryPrime);
                var ps = session.Prepare(Query);
                Assert.NotNull(ps);
                Assert.AreSame(ps, session.Prepare(Query));
                Assert.AreNotSame(ps, session.Prepare("SELECT * FROM system.local"));
            }
        }
        
        private class OrderedLoadBalancingPolicy : ILoadBalancingPolicy
        {
            private ICollection<Host> _hosts;

            public void Initialize(ICluster cluster)
            {
                _hosts = cluster.AllHosts();
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                return _hosts;
            }
        }
    }
}