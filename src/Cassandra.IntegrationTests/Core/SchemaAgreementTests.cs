namespace Cassandra.IntegrationTests.Core
{
    using System.Linq;
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

        private const int MaxSchemaAgreementWaitSeconds = 3;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint)
                              .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(60000))
                              .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                              .WithMaxSchemaAgreementWaitSeconds(MaxSchemaAgreementWaitSeconds)
                              .Build();
            _session = (Session) _cluster.Connect();
            _session.CreateKeyspace(KeyspaceName, null, false);
            _session.ChangeKeyspace(KeyspaceName);
        }
        
        [Test]
        public async Task Should_CheckSchemaAgreementReturnTrue_When_AllNodesUpAndSchemaWasNotChanged()
        {
            Assert.IsTrue(_cluster.Metadata.Hosts.All(h => h.IsUp));
            Assert.IsTrue(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnFalse_When_OneNodeIsDown()
        {
            try
            {
                TestCluster.PauseNode(1);
                Assert.IsFalse(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
            }
            finally
            {
                TestCluster.ResumeNode(1);
                TestUtils.WaitForSchemaAgreement(_cluster, false, true);
            }
        }
        
        [Test]
        public async Task Should_SchemaInAgreementReturnTrue_When_ADdlStatementIsExecutedAndAllNodesUp()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            
            var cql = new SimpleStatement(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
            var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
            Assert.IsTrue(rowSet.Info.SchemaInAgreement);
        }
        
        [Test]
        public async Task Should_SchemaInAgreementReturnTrue_When_ADmlStatementIsExecutedAlthoughOneNodeIsDown()
        {
            try
            {
                TestCluster.PauseNode(1);
                var cql = new SimpleStatement("SELECT count(*) FROM system.peers");
                var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
                Assert.Greater(rowSet.First().GetValue<long>(0), 0);
                Assert.IsTrue(rowSet.Info.SchemaInAgreement);
            }
            finally
            {
                TestCluster.ResumeNode(1);
                TestUtils.WaitForSchemaAgreement(_cluster, false, true);
            }
        }

        [Test]
        public async Task Should_SchemaInAgreementReturnFalse_When_OneNodeIsDown()
        {
            try
            {
                TestCluster.PauseNode(1);
                var tableName = TestUtils.GetUniqueTableName().ToLower();

                var cql = new SimpleStatement(
                    $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
                var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
                Assert.IsFalse(rowSet.Info.SchemaInAgreement);
            }
            finally
            {
                TestCluster.ResumeNode(1);
                TestUtils.WaitForSchemaAgreement(_cluster, false, true);
            }
        }
    }
}