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
using System.Linq;
using System.Collections.Generic;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BatchStatements : SharedClusterTest
    {
        private readonly string _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();

        /// <summary>
        /// Use a 3-node cluster to test prepared batches (unprepared flow).
        /// </summary>
        public BatchStatements() : base(!IsAppVeyor ? 3 : 1)
        {
            
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            CreateTable(_tableName);
            // It should be unprepared on some of the nodes, we use a different table from the rest of the tests 
            CreateTable("tbl_unprepared_flow");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement()
        {
            var expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            foreach (var val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            Session.Execute(batch);

            // Verify results
            var rs = Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({1}, {2}, {3})");

            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_With_Unprepared_Flow()
        {
            // Use a dedicated cluster and table
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .WithQueryOptions(new QueryOptions().SetPrepareOnAllHosts(false)).Build())
            {
                var session = cluster.Connect(KeyspaceName);
                var ps1 = session.Prepare("INSERT INTO tbl_unprepared_flow (id, label) VALUES (?, ?)");
                var ps2 = session.Prepare("UPDATE tbl_unprepared_flow SET label = ? WHERE id = ?");
                session.Execute(new BatchStatement()
                    .Add(ps1.Bind(1, "label1_u"))
                    .Add(ps2.Bind("label2_u", 2)));
                // Execute in multiple nodes
                session.Execute(new BatchStatement()
                    .Add(ps1.Bind(3, "label3_u"))
                    .Add(ps2.Bind("label4_u", 4)));
                var result = session.Execute("SELECT id, label FROM tbl_unprepared_flow")
                                    .Select(r => new object[] { r.GetValue<int>(0), r.GetValue<string>(1) })
                                    .OrderBy(arr => (int)arr[0])
                                    .ToArray();
                Assert.AreEqual(Enumerable.Range(1, 4).Select(i => new object[] { i, $"label{i}_u"}), result);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_AsyncTest()
        {
            var expectedValues = new List<object[]> { new object[] { 10, "label1", 1 }, new object[] { 11, "label2", 2 }, new object[] { 12, "label3", 3 } };

            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            foreach (var val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            Assert.DoesNotThrow(() => Session.ExecuteAsync(batch).Wait());
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatementSingle()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var batch = new BatchStatement();
            var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({20}, '{"label 20"}', {20})");
            batch.Add(simpleStatement);
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({20})");
            var row = rs.First();
            CollectionAssert.AreEqual(row, new object[] { 20, "label 20", 20});
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Bound()
        {
            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            #pragma warning disable 618
            batch.Add(simpleStatement.Bind(100, "label 100", 10000));
            #pragma warning restore 618
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({100})");
            var row = rs.First();
            CollectionAssert.AreEqual(row, new object[] {100, "label 100", 10000});
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Multiple()
        {
            var expectedValues = new List<object[]>();

            var batch = new BatchStatement();

            for (var x = 200; x < 205; x++)
            {
                var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({x}, '{"label" + x}', {x * x})");
                expectedValues.Add(new object[] { x, "label" + x, x * x });
                batch.Add(simpleStatement);
            }
            Session.Execute(batch);

            var rs = Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({"200, 201, 202, 203, 204"})");

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables()
        {
            var batch = new BatchStatement();

            CreateTwoTableTestEnv("table1", "table2");

            batch.Add(new SimpleStatement($@"INSERT INTO table1 (id, label, number) VALUES ({1}, '{"label1"}', {1})"));
            batch.Add(new SimpleStatement($@"INSERT INTO table2 (id, label, number) VALUES ({2}, '{"label2"}', {2})"));

            Session.Execute(batch);

            //Verify Results
            var rsTable1 = Session.Execute("SELECT * FROM table1");
            VerifyData(rsTable1, new List<object[]> { new object[] { 1, "label1", 1 } });

            var rsTable2 = Session.Execute("SELECT * FROM table2");
            VerifyData(rsTable2, new List<object[]> { new object[] { 2, "label2", 2 } });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables_OneInvalidTable()
        {
            var batch = new BatchStatement();

            batch.Add(new SimpleStatement($@"INSERT INTO {_tableName} (id, label, number) VALUES ({400}, '{"label1"}', {1})"));
            batch.Add(new SimpleStatement($@"INSERT INTO table_randomnonexistent (id, label, number) VALUES ({2}, '{"label2"}', {2})"));

            Assert.Throws<InvalidQueryException>(() => Session.Execute(batch));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_MixedStatement()
        {
            var simpleStatement =
                new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({500}, {"label 500"}, {2})");
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batchStatement = new BatchStatement();
            var expectedValues = new List<object[]> { new object[] { 500, "label 500", 2 }, new object[] { 501, "test", 2 } };

            batchStatement.Add(ps.Bind(501, "test", 2));
            batchStatement.Add(simpleStatement);

            var rs = Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({500}, {501})");
            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_SerialConsistency()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");

            Assert.Throws<ArgumentException>(() =>
            {
                //You can not specify local serial consistency as a valid read one.
                var batch = new BatchStatement()
                    .Add(query)
                    .SetBatchType(BatchType.Logged)
                    .SetSerialConsistencyLevel(ConsistencyLevel.Quorum);
                Session.Execute(batch);
            });

            //It should work
            var statement = new BatchStatement()
                .Add(query)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);

            //Read consistency specified and write consistency specified
            Session.Execute(statement);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_GeneratedTimestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");
            var generator = new MockTimestampGenerator();
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions()))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithTimestampGenerator(generator).Build())
            {
                var session = cluster.Connect();
                var batchStatement = new BatchStatement().Add(query);
                session.Execute(batchStatement);
                var timestamp = generator.Next();
                var executed = simulacronCluster.GetQueries(null, "BATCH");
                Assert.IsNotEmpty(executed);
                var executedArray = executed.ToArray();
                Assert.AreEqual(1, executedArray.Length);
                var log = executedArray[0];
                var logtimestamp = (long) log.client_timestamp;
                Assert.AreEqual(timestamp, logtimestamp);
            }
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_DefaultGeneratedTimestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions()))
            using (var cluster = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var oldTimestamp = cluster.Configuration.Policies.TimestampGenerator.Next();
                var batchStatement = new BatchStatement().Add(query);
                session.Execute(batchStatement);
                var executed = simulacronCluster.GetQueries(null, "BATCH");
                Assert.IsNotEmpty(executed);
                var executedArray = executed.ToArray();
                Assert.AreEqual(1, executedArray.Length);
                var log = executedArray[0];
                var logtimestamp = (long) log.client_timestamp;
                Assert.Greater(logtimestamp, oldTimestamp);
            }
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_Timestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");

            Assert.DoesNotThrow(() =>
            {
                //It should work
                var statement = new BatchStatement()
                    .Add(query)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum)
                    .SetTimestamp(DateTime.Now);

                //Read consistency specified and write consistency specified
                Session.Execute(statement);
            });
        }

        [Test]
        [TestCassandraVersion(2, 0, Comparison.Equal)]
        public void Batch_PreparedStatements_FlagsNotSupportedInC2_0()
        {
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            batch.Add(ps.Bind(1, "label1", 1));
            Assert.Throws<NotSupportedException>(() => Session.Execute(batch.SetTimestamp(DateTime.Now)));
        }

        [Test]
        [TestCassandraVersion(1, 9, Comparison.LessThan)]
        public void Batch_PreparedStatements_NotSupportedInC1_2()
        {
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            batch.Add(ps.Bind(1, "label1", 1));
            try
            {
                Session.Execute(batch);
                Assert.Fail("Cassandra version below 2.0, should not execute batches of prepared statements");
            }
            catch (NotSupportedException ex)
            {
                //This is OK
                Assert.True(ex.Message.ToLower().Contains("batch"));
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_Large()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N");

            CreateTable(tableName);


            var ps = Session.Prepare($@"INSERT INTO {tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            var expectedValues = new List<object[]>();

            for (var x = 1; x <= 100; x++)
            {
                expectedValues.Add(new object[] { x, "value" + x, x });
                batch.Add(ps.Bind(new object[] { x, "value" + x, x }));
            }

            Session.Execute(batch);

            // Verify correct values
            var rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        private static void VerifyData(IEnumerable<Row> rowSet, List<object[]> expectedValues)
        {
            var x = 0;
            foreach (var row in rowSet)
            {
                var y = 0;
                var objArr = expectedValues[x];

                using (var rowEnum = row.GetEnumerator())
                {
                    while (rowEnum.MoveNext())
                    {
                        var current = rowEnum.Current;
                        Assert.AreEqual(objArr[y], current, $"Found difference between expected and actual row {objArr[y]} != {current}");
                        y++;
                    }
                }
                x++;
            }
        }

        private void CreateTable(string tableName)
        {
            CreateTable(Session, tableName);
        }

        private void CreateTable(ISession session, string tableName)
        {
            QueryTools.ExecuteSyncNonQuery(session, $@"CREATE TABLE {tableName}(
                                                                id int PRIMARY KEY,
                                                                label text,
                                                                number int
                                                                );");
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }


        private void CreateTwoTableTestEnv(string table1, string table2)
        {
            QueryTools.ExecuteSyncNonQuery(Session, $@"CREATE TABLE {table1} (
                                                                          id int PRIMARY KEY,
                                                                          label text,
                                                                          number int
                                                                          );");

            QueryTools.ExecuteSyncNonQuery(Session, $@"CREATE TABLE {table2} (
                                                                        id int PRIMARY KEY,
                                                                        label text,
                                                                        number int
                                                                        );");
            TestUtils.WaitForSchemaAgreement(Session.Cluster);
        }
    }

    //Mock generator
    class MockTimestampGenerator : ITimestampGenerator
    {
        private DateTime _initialDateTime = DateTime.Parse("2017-01-01T00:00:01-3:00");
        private const long TicksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;
        public long Next()
        {
            return (_initialDateTime.Ticks - TypeSerializer.UnixStart.UtcTicks) / TicksPerMicrosecond;
        }
    }
}