using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class TimestampTests : SharedClusterTest
    {
        public TimestampTests() : base(1, false)
        {
            
        }

        [Test]
        public void Cluster_Uses_Provided_Timestamp_Generator()
        {
            var generator = new TestTimestampGenerator();
            using (var cluster = Cluster.Builder()
                                        .WithTimestampGenerator(generator)
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                TestHelper.ParallelInvoke(() => session.Execute("SELECT * FROM system.local"), 10);
                // The driver should use the generator against C* 2.1+
                Assert.AreEqual(GetProtocolVersion() < ProtocolVersion.V3 ? 0 : 10, generator.GetCounter());
            }
        }

        [Test]
        public void Should_Use_Statement_Timestamp_Precedence_Over_Cluster_Timestamp_Generator()
        {
            var generator = new TestTimestampGenerator();
            using (var cluster = Cluster.Builder()
                                        .WithTimestampGenerator(generator)
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                var stmt = new SimpleStatement("SELECT * FROM system.local");
                stmt.SetTimestamp(DateTimeOffset.Now);
                if (GetProtocolVersion() < ProtocolVersion.V3)
                {
                    Assert.Throws<NotSupportedException>(() => session.Execute(stmt));
                }
                else
                {
                    session.Execute(stmt);
                }
                // The driver should use the generator against C* 2.1+
                Assert.AreEqual(0, generator.GetCounter());
            }
        }

        [Test]
        public void Should_Use_CQL_Timestamp_Precedence_Over_Cluster_Timestamp_Generator()
        {
            var generator = new TestTimestampGenerator();
            using (var cluster = Cluster.Builder()
                                        .WithTimestampGenerator(generator)
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                session.CreateKeyspace("timestamp_tests",null, false);
                session.ChangeKeyspace("timestamp_tests");
                QueryTools.ExecuteSyncNonQuery(session, string.Format(@"
                CREATE TABLE {0}(
                id uuid PRIMARY KEY,
                label text);", "tbl_timestamp_test"));
                var now = DateTimeOffset.Parse("2017-01-01T00:00:00.000+00:00");
                var id = Guid.NewGuid();
                var stmt = new SimpleStatement("INSERT INTO tbl_timestamp_test (id, label) VALUES (?,?) USING TIMESTAMP ?",
                    id, "test label", now.Ticks);
                session.Execute(stmt);
                // The driver should use the generator against C* 2.1+
                Assert.AreEqual(CassandraVersion < new Version(2, 1) ? 0 : 4, generator.GetCounter());

                //verify if the write time was according to USING TIMESTAMP now timestamp generator
                var writetimeStmt = new SimpleStatement("select writetime(label) as insert_timestamp from tbl_timestamp_test where id = ?",
                    id);
                var rs = session.Execute(writetimeStmt);
                Assert.NotNull(rs);
                var first = rs.FirstOrDefault();
                Assert.NotNull(first);
                Assert.AreEqual(now.Ticks, first.GetValue<Int64>("insert_timestamp"));
            }
        }

        private class TestTimestampGenerator : ITimestampGenerator
        {
            private int _counter;

            public long GetCounter()
            {
                return Volatile.Read(ref _counter);
            }

            public long Next()
            {
                return Interlocked.Increment(ref _counter);
            }
        }
    }
}
