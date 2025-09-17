// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class ClusterPeersV2SimulacronTests : SimulacronTest
    {
        public ClusterPeersV2SimulacronTests() : base(
            options: new SimulacronOptions { Nodes = "3" },
            simulacronManager: SimulacronManager.GetForPeersTests())
        {
        }

        [Test]
        public void Should_SendRequestsToAllHosts_When_PeersOnSameAddress()
        {
            const string query = "SELECT * FROM ks.table";

            Assert.AreEqual(3, Session.Cluster.AllHosts().Count);
            Assert.IsTrue(Session.Cluster.AllHosts().All(h => h.IsUp));
            Assert.AreEqual(1, Session.Cluster.AllHosts().Select(h => h.Address.Address).Distinct().Count());
            Assert.AreEqual(3, Session.Cluster.AllHosts().Select(h => h.Address.Port).Distinct().Count());

            foreach (var i in Enumerable.Range(0, 10))
            {
                // doesn't exist but doesn't matter
                Session.Execute(query);
            }

            Assert.AreEqual(3, Session.Cluster.AllHosts().Count);
            Assert.IsTrue(Session.Cluster.AllHosts().All(h => h.IsUp));

            var queriesByNode = TestCluster.GetNodes().Select(n => n.GetQueries(query, QueryType.Query));
            Assert.IsTrue(queriesByNode.All(queries => queries.Count >= 1));
        }
    }
}