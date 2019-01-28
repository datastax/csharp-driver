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
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            Assert.Throws<KeyNotFoundException>(() => Cluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName));
            var oldTokenMap = Cluster.Metadata.TokenToReplicasMap;
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Assert.AreEqual(3, Cluster.Metadata.Hosts.Count);

            Session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);
            Session.ChangeKeyspace(keyspaceName);

            TestHelper.RetryAssert(() =>
            {
                IReadOnlyDictionary<IToken, ISet<Host>> replicas = null;
                Assert.DoesNotThrow(() =>
                {
                    replicas = Cluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                });
                Assert.AreEqual(Cluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), replicas.Count);
            });
            Assert.IsTrue(object.ReferenceEquals(Cluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsRemoved()
        {
            var oldTokenMap = Cluster.Metadata.TokenToReplicasMap;
            var removeKeyspaceCql = $"DROP KEYSPACE {KeyspaceName}";
            Session.Execute(removeKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);

            TestHelper.RetryAssert(() =>
            {
                Assert.Throws<KeyNotFoundException>(() => Cluster.Metadata.TokenToReplicasMap.GetByKeyspace(KeyspaceName));
            });
            Assert.IsTrue(object.ReferenceEquals(Cluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsChanged()
        {
            Assert.AreEqual(3, Cluster.Metadata.Hosts.Count);
            var oldTokenMap = Cluster.Metadata.TokenToReplicasMap;
            var alterKeyspaceCql = $"ALTER KEYSPACE {KeyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Session.Execute(alterKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);
            TestHelper.RetryAssert(() =>
            {
                var replicas = Cluster.Metadata.TokenToReplicasMap.GetByKeyspace(KeyspaceName);
                Assert.AreEqual(Cluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), replicas.Count);
                Assert.AreEqual(3, Cluster.Metadata.GetReplicas(KeyspaceName, Encoding.UTF8.GetBytes("123")).Count);
            });
            Assert.IsTrue(object.ReferenceEquals(Cluster.Metadata.TokenToReplicasMap, oldTokenMap));
        }
    }
}