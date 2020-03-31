// 
//       Copyright DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tests;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Unit;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category(TestCategory.Long), TestCassandraVersion(3, 0)]
    public class ExceptionLongTests : TestGlobals
    {
        private ITestCluster _testCluster;
        private ICluster _cluster;
        private ISession _session;

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _testCluster = TestClusterManager.CreateNew(2, new TestClusterOptions
            {
                CassandraYaml = new[] {"tombstone_failure_threshold:1000"},
                JvmArgs = new[] {"-Dcassandra.test.fail_writes_ks=ks_writes"}
            });

            _cluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build();
            _session = _cluster.Connect();
            _session.Execute("CREATE KEYSPACE ks_reads" +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
            _session.Execute("CREATE KEYSPACE ks_writes" +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2}");
            _session.Execute("CREATE TABLE ks_reads.read_fail_tbl(pk int, cc int, v int, primary key (pk, cc))");
            _session.Execute("CREATE TABLE ks_writes.write_fail_tbl(pk int PRIMARY KEY, v int)");
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _cluster.Dispose();
            _testCluster.Remove();
        }

        [Test]
        public async Task Should_Throw_A_ReadFailureException_When_Tombstone_Overwhelmed_On_Replica()
        {
            var ps = await _session.PrepareAsync("INSERT INTO ks_reads.read_fail_tbl (pk, cc, v) VALUES (?, ?, ?)");
            var index = 0;
            await TestHelper.TimesLimit(
                () => _session.ExecuteAsync(ps.Bind(1, Interlocked.Increment(ref index), null)), 2000, 256);

            var selectStatement = new SimpleStatement("SELECT * FROM ks_reads.read_fail_tbl WHERE pk = 1");
            var ex = Assert.ThrowsAsync<ReadFailureException>(() => _session.ExecuteAsync(selectStatement));
            Assert.AreEqual(1, ex.Failures);
            Assert.AreEqual(0, ex.ReceivedAcknowledgements);

            var protocolVersion = (ProtocolVersion) _session.BinaryProtocolVersion;
            if (protocolVersion.SupportsFailureReasons())
            {
                Assert.AreEqual(1, ex.Reasons.Count);
            }
            else
            {
                // A not null, empty dictionary
                Assert.AreEqual(0, ex.Reasons?.Count);
            }
        }

        [Test]
        public void Should_Throw_WriteFailureException_When_Obtained_In_The_Response()
        {
            var statement = new SimpleStatement("INSERT INTO ks_writes.write_fail_tbl (pk, v) VALUES (1, 1)")
                .SetConsistencyLevel(ConsistencyLevel.All);
            var ex = Assert.Throws<WriteFailureException>(() => _session.Execute(statement));
            Assert.Greater(ex.Failures, 0);
            Assert.AreEqual("SIMPLE", ex.WriteType);

            var protocolVersion = (ProtocolVersion) _session.BinaryProtocolVersion;
            if (protocolVersion.SupportsFailureReasons())
            {
                Assert.Greater(ex.Reasons.Count, 0);
            }
            else
            {
                // A not null, empty dictionary
                Assert.AreEqual(0, ex.Reasons?.Count);
            }
        }
    }
}