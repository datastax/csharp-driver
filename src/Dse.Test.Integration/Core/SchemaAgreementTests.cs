using System;
using System.Diagnostics;
using System.Threading.Tasks;

using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Unit;

using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category("short"), Category("realcluster")]
    public class SchemaAgreementTests : SharedClusterTest
    {
        private volatile bool _paused = false;

        public SchemaAgreementTests() : base(2, false, true)
        {
        }

        private Cluster _cluster;
        private Session _session;

        private const int MaxSchemaAgreementWaitSeconds = 10;

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

        // ordering for efficiency, it's not required
        [Test, Order(1)]
        public async Task Should_CheckSchemaAgreementReturnTrueAndSchemaInAgreementReturnTrue_When_AllNodesUp()
        {
            var listener = new TestTraceListener();
            var level = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            Trace.Listeners.Add(listener);
            try
            {
                if (_paused)
                {
                    TestCluster.ResumeNode(2);
                }

                //// this test can't be done with simulacron because there's no support for schema_changed responses
                var tableName = TestUtils.GetUniqueTableName().ToLower();

                var cql = new SimpleStatement(
                    $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
                var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
                Assert.IsTrue(rowSet.Info.IsSchemaInAgreement, "is in agreement");
                await TestHelper.RetryAssertAsync(
                    async () =>
                    {
                        Assert.IsTrue(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false), "check");
                    },
                    100,
                    50).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Trace.Flush();
                Assert.Fail("Exception: " + ex.ToString() + Environment.NewLine + string.Join(Environment.NewLine, listener.Queue.ToArray()));
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                Diagnostics.CassandraTraceSwitch.Level = level;
            }
        }

        // ordering for efficiency, it's not required
        [Test, Order(2)]
        public async Task Should_CheckSchemaAgreementReturnFalseAndSchemaInAgreementReturnFalse_When_OneNodeIsDown()
        {
            //// this test can't be done with simulacron because there's no support for schema_changed responses
            _paused = true;
            TestCluster.PauseNode(2);

            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cql = new SimpleStatement(
                $"CREATE TABLE {tableName} (id int PRIMARY KEY, description text)");
            var rowSet = await _session.ExecuteAsync(cql).ConfigureAwait(false);
            Assert.IsFalse(rowSet.Info.IsSchemaInAgreement);
            Assert.IsFalse(await _cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
        }

        public override void OneTimeTearDown()
        {
            _cluster.Dispose();
            base.OneTimeTearDown();
        }
    }
}