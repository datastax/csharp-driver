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

using System.Collections.Generic;
using System.Linq;
using System.Text;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category("short")]
    public class TokenMapSchemaChangeTests : SharedClusterTest
    {
        public TokenMapSchemaChangeTests() : base(3, true, true, new TestClusterOptions { UseVNodes = true })
        {
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsCreated()
        {
            TestUtils.WaitForSchemaAgreement(Cluster);
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var newSession = GetNewSession();
            var newCluster = newSession.Cluster;
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            Assert.AreEqual(3, newCluster.Metadata.Hosts.Count);

            Assert.IsNull(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName));
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";

            newSession.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            newSession.ChangeKeyspace(keyspaceName);

            TestHelper.RetryAssert(() =>
            {
                IReadOnlyDictionary<IToken, ISet<Host>> replicas = null;
                Assert.DoesNotThrow(() =>
                {
                    replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                });
                Assert.AreEqual(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), replicas.Count);
            });
            Assert.IsTrue(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsRemoved()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);

            var newSession = GetNewSession();
            var newCluster = newSession.Cluster;
            var removeKeyspaceCql = $"DROP KEYSPACE {keyspaceName}";
            newSession.Execute(removeKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            TestHelper.RetryAssert(() =>
            {
                Assert.IsNull(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName));
            });
            Assert.IsTrue(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsChanged()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);

            var newSession = GetNewSession(keyspaceName);
            var newCluster = newSession.Cluster;
            TestHelper.RetryAssert(() =>
            {
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.AreEqual(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), replicas.Count);
                Assert.AreEqual(3, newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count);
            });

            Assert.AreEqual(3, newCluster.Metadata.Hosts.Count(h => h.IsUp));
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            var alterKeyspaceCql = $"ALTER KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 2}}";
            newSession.Execute(alterKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            TestHelper.RetryAssert(() =>
            {
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.AreEqual(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), replicas.Count);
                Assert.AreEqual(2, newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count);
            });

            Assert.AreEqual(3, newCluster.Metadata.Hosts.Count(h => h.IsUp));
            Assert.IsTrue(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }
    }
}