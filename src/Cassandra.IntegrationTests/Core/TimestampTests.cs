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
        public void Should_UseProvidedTimestamp_When_TimestampIsNotAValidDateTimeOffset()
        {
            var generator = new TicksTimestampGenerator();
            var unixEpochStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
            using (var cluster = Cluster.Builder()
                                        .WithTimestampGenerator(generator)
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                var ks = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
                var table = TestUtils.GetUniqueTableName().ToLowerInvariant();
                session.CreateKeyspaceIfNotExists(ks);
                session.ChangeKeyspace(ks);
                session.Execute($"CREATE TABLE IF NOT EXISTS {table} (id int primary key, random_value text)");
                var insertPrepare = session.Prepare($"INSERT INTO {table} (id, random_value) VALUES (?, ?)");

                var dt = DateTimeOffset.UtcNow;
                var stmt1 = insertPrepare.Bind(1, "123");
                var stmt2 = insertPrepare.Bind(2, "321").SetTimestamp(dt);

                if (GetProtocolVersion() < ProtocolVersion.V3)
                {
                    Assert.Throws<NotSupportedException>(() => session.Execute(stmt1));
                    Assert.Throws<NotSupportedException>(() => session.Execute(stmt2));
                }
                else
                {
                    session.Execute(stmt1);
                    var ticksStmt1 = generator.GetTicks();
                    session.Execute(stmt2);

                    var rs1 = session.Execute($"SELECT WRITETIME(random_value) FROM {table} WHERE id = {1}");
                    var rs2 = session.Execute($"SELECT WRITETIME(random_value) FROM {table} WHERE id = {2}");

                    Assert.AreEqual(6, generator.GetCounter()); // 2x selects, 1x insert (the other has a statement timestamp), create keyspace, use keyspace, create table
                    Assert.AreEqual(ticksStmt1, rs1.Single().GetValue<long>(0));
                    Assert.AreEqual((dt - unixEpochStart).Ticks / 10, rs2.Single().GetValue<long>(0));
                }
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

        private class TicksTimestampGenerator : ITimestampGenerator
        {
            private int _counter;
            private long _ticks;

            public long GetCounter()
            {
                return Volatile.Read(ref _counter);
            }

            public long GetTicks()
            {
                return Volatile.Read(ref _ticks);
            }

            public long Next()
            {
                var ticks = DateTime.UtcNow.Ticks;
                Interlocked.Increment(ref _counter);
                Interlocked.Exchange(ref _ticks, ticks);
                return ticks;
            }
        }
    }
}
