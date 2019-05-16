//
//       Copyright DataStax, Inc.
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
//

using System.Text;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category("short")]
    public class TokenMapTopologyChangeTests
    {
        private ITestCluster TestCluster { get; set; }
        private ICluster ClusterObj { get; set; }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TokenMap_Should_RebuildTokenMap_When_NodeIsDecommissioned(bool metadataSync)
        {
            TestCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            ClusterObj = Cluster.Builder()
                                .AddContactPoint(TestCluster.InitialContactPoint)
                                .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync))
                                .Build();

            var session = ClusterObj.Connect();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";

            session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(ClusterObj);
            session.ChangeKeyspace(keyspaceName);

            var replicas = ClusterObj.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
            Assert.AreEqual(metadataSync ? 3 : 1, replicas.Count);
            Assert.AreEqual(3, ClusterObj.Metadata.Hosts.Count);
            var oldTokenMap = ClusterObj.Metadata.TokenToReplicasMap;
            this.TestCluster.DecommissionNode(1);
            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(2, ClusterObj.Metadata.Hosts.Count);
                replicas = ClusterObj.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                Assert.AreEqual(metadataSync ? 2 : 1, replicas.Count);
            }, 100, 150);
            Assert.IsFalse(object.ReferenceEquals(ClusterObj.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TokenMap_Should_RebuildTokenMap_When_NodeIsBootstrapped(bool metadataSync)
        {
            TestCluster = TestClusterManager.CreateNew(2, new TestClusterOptions { UseVNodes = true });
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            ClusterObj = Cluster.Builder()
                                .AddContactPoint(TestCluster.InitialContactPoint)
                                .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync))
                                .Build();

            var session = ClusterObj.Connect();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";

            session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(ClusterObj);
            session.ChangeKeyspace(keyspaceName);

            var replicas = ClusterObj.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
            Assert.AreEqual(metadataSync ? 2 : 1, replicas.Count);
            Assert.AreEqual(2, ClusterObj.Metadata.Hosts.Count);
            var oldTokenMap = ClusterObj.Metadata.TokenToReplicasMap;
            this.TestCluster.BootstrapNode(3);
            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(3, ClusterObj.Metadata.Hosts.Count);
                replicas = ClusterObj.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                Assert.AreEqual(metadataSync ? 3 : 1, replicas.Count);
            }, 100, 150);
            Assert.IsFalse(object.ReferenceEquals(ClusterObj.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [TearDown]
        public void TearDown()
        {
            TestCluster?.Remove();
            ClusterObj?.Shutdown();
        }
    }
}