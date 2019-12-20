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
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category("short"), Category("realcluster")]
    public class TokenMapTopologyChangeTests
    {
        private ITestCluster TestCluster { get; set; }
        private ICluster ClusterObjSync { get; set; }
        private ICluster ClusterObjNotSync { get; set; }

        [Test]
        public void TokenMap_Should_RebuildTokenMap_When_NodeIsDecommissioned()
        {
            TestCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            ClusterObjSync = Cluster.Builder()
                                .AddContactPoint(TestCluster.InitialContactPoint)
                                .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(true))
                                .Build();

            ClusterObjNotSync = Cluster.Builder()
                                .AddContactPoint(TestCluster.InitialContactPoint)
                                .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                .Build();

            var sessionNotSync = ClusterObjNotSync.Connect();
            var sessionSync = ClusterObjSync.Connect();

            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            sessionNotSync.Execute(createKeyspaceCql);

            TestUtils.WaitForSchemaAgreement(ClusterObjNotSync);
            TestUtils.WaitForSchemaAgreement(ClusterObjSync);

            sessionNotSync.ChangeKeyspace(keyspaceName);
            sessionSync.ChangeKeyspace(keyspaceName);

            var replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
            var replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
            Assert.AreEqual(3, replicasSync.Count);
            Assert.AreEqual(1, replicasNotSync.Count);

            Assert.AreEqual(3, ClusterObjSync.Metadata.Hosts.Count);
            Assert.AreEqual(3, ClusterObjNotSync.Metadata.Hosts.Count);

            var oldTokenMapNotSync = ClusterObjNotSync.Metadata.TokenToReplicasMap;
            var oldTokenMapSync = ClusterObjSync.Metadata.TokenToReplicasMap;

            if (TestClusterManager.SupportsDecommissionForcefully())
            {
                this.TestCluster.DecommissionNodeForcefully(1);
            }
            else
            {
                this.TestCluster.DecommissionNode(1);
            }

            this.TestCluster.Remove(1);

            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(2, ClusterObjSync.Metadata.Hosts.Count);
                Assert.AreEqual(2, ClusterObjNotSync.Metadata.Hosts.Count);

                replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                Assert.AreEqual(2, replicasSync.Count);
                Assert.AreEqual(1, replicasNotSync.Count);

                Assert.IsFalse(object.ReferenceEquals(ClusterObjNotSync.Metadata.TokenToReplicasMap, oldTokenMapNotSync));
                Assert.IsFalse(object.ReferenceEquals(ClusterObjSync.Metadata.TokenToReplicasMap, oldTokenMapSync));
            }, 100, 150);

            this.TestCluster.BootstrapNode(4);
            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(3, ClusterObjSync.Metadata.Hosts.Count);
                Assert.AreEqual(3, ClusterObjNotSync.Metadata.Hosts.Count);

                replicasSync = ClusterObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                replicasNotSync = ClusterObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                Assert.AreEqual(3, replicasSync.Count);
                Assert.AreEqual(1, replicasNotSync.Count);

                Assert.IsFalse(object.ReferenceEquals(ClusterObjNotSync.Metadata.TokenToReplicasMap, oldTokenMapNotSync));
                Assert.IsFalse(object.ReferenceEquals(ClusterObjSync.Metadata.TokenToReplicasMap, oldTokenMapSync));
            }, 500, 360);
        }

        [TearDown]
        public void TearDown()
        {
            ClusterObjSync?.Shutdown();
            ClusterObjNotSync?.Shutdown();
            TestCluster?.Remove();
        }
    }
}