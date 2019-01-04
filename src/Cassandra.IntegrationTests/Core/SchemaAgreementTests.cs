namespace Cassandra.IntegrationTests.Core
{
    using System.Threading.Tasks;

    using Cassandra.IntegrationTests.TestBase;

    using NUnit.Framework;

    [TestFixture, Category("short")]
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
                              .WithMaxSchemaAgreementWaitSeconds(MaxSchemaAgreementWaitSeconds)
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
            TestUtils.WaitForSchemaAgreement(_cluster, false, true, MaxTestSchemaAgreementRetries);
        }
    }
}