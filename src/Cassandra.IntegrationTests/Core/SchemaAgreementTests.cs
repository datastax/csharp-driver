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

using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short"), Category("realcluster")]
    public class SchemaAgreementTests : SharedClusterTest
    {
        public SchemaAgreementTests() : base(2, false, true)
        {
        }

        private Cluster _cluster;
        private Session _session;

        private const int MaxSchemaAgreementWaitSeconds = 30;

        private const int MaxTestSchemaAgreementRetries = 240;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint)
                              .WithSocketOptions(new SocketOptions()
                                                 .SetReadTimeoutMillis(15000)
                                                 .SetConnectTimeoutMillis(60000))
                              .WithMaxSchemaAgreementWaitSeconds(SchemaAgreementTests.MaxSchemaAgreementWaitSeconds)
                              .Build();
            _session = (Session)_cluster.Connect();
            _session.CreateKeyspace(KeyspaceName, null, false);
            _session.ChangeKeyspace(KeyspaceName);
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnFalse_When_ADdlStatementIsExecutedAndOneNodeIsDown()
        {
            //// this test can't be done with simulacron because there's no support for schema_changed responses
            TestCluster.PauseNode(2);

            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cql = new SimpleStatement(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
            await _session.ExecuteAsync(cql).ConfigureAwait(false);
            Assert.IsFalse(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task Should_SchemaInAgreementReturnTrue_When_ADdlStatementIsExecutedAndAllNodesUp()
        {
            //// this test can't be done with simulacron because there's no support for schema_changed responses
            var tableName = TestUtils.GetUniqueTableName().ToLower();

            var cql = new SimpleStatement(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
            var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
            Assert.IsTrue(rowSet.Info.IsSchemaInAgreement);
        }

        [Test]
        public async Task Should_SchemaInAgreementReturnFalse_When_ADdlStatementIsExecutedAndOneNodeIsDown()
        {
            //// this test can't be done with simulacron because there's no support for schema_changed responses
            TestCluster.PauseNode(2);
            var tableName = TestUtils.GetUniqueTableName().ToLower();

            var cql = new SimpleStatement(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
            var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
            Assert.IsFalse(rowSet.Info.IsSchemaInAgreement);
        }

        [TearDown]
        public void TearDown()
        {
            TestCluster.ResumeNode(2);
            TestUtils.WaitForSchemaAgreement(_cluster, false, true, SchemaAgreementTests.MaxTestSchemaAgreementRetries);
        }
        
        public override void OneTimeTearDown()
        {
            _session.Dispose();
            _cluster.Dispose();
            base.OneTimeTearDown();
        }
    }
}