//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
﻿using Cassandra.IntegrationTests.TestBase;
﻿using Cassandra.IntegrationTests.TestClusterManagement;
﻿using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BatchStatements : TestGlobals
    {
        private ISession _session;
        private ITestCluster _testCluster;

        [SetUp]
        public void SetupFixture()
        {
            _testCluster = TestClusterManager.GetTestCluster(4);
            _session = _testCluster.Session;
        }

        /// <summary>
        /// The protocol versions in which Batches are supported
        /// </summary>
        private static readonly int[] ProtocolVersionSupported = { 2, 3 };

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };
            CreateTable(tableName);

            PreparedStatement ps = _session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            _session.Execute(batch);

            // Verify results
            RowSet rs = _session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_AsyncTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };
            CreateTable(tableName);

            PreparedStatement ps = _session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            var task = _session.ExecuteAsync(batch);
            var rs = task.Result;

            // Verify results
            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatementSingle()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var batch = new BatchStatement();
            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, 1, "label 1", 10));
            batch.Add(simpleStatement);
            _session.Execute(batch);

            //Verify Results
            var rs = _session.Execute("SELECT * FROM " + tableName);
            var row = rs.First();
            Assert.True(row != null, "There should be a row stored.");
            Assert.True(row.SequenceEqual(new object[] { 1, "label 1", 10 }), "Stored values dont match");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_With_Query_Values()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);
            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName), 100, "label 100", 10000);
            batch.Add(simpleStatement);
            _session.Execute(batch);

            //Verify Results
            var rs = _session.Execute("SELECT * FROM " + tableName);
            var row = rs.First();
            Assert.True(row != null, "There should be a row stored.");
            Assert.True(row.SequenceEqual(new object[] { 100, "label 100", 10000 }), "Stored values dont match");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Multiple()
        {
            SimpleStatement simpleStatement = null;
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var expectedValues = new List<object[]>();

            CreateTable(tableName);

            BatchStatement batch = new BatchStatement();

            for (var x = 1; x <= 5; x++)
            {
                simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, x, "label" + x, x * x));
                expectedValues.Add(new object[] { x, "label" + x, x * x });
                batch.Add(simpleStatement);
            }
            _session.Execute(batch);

            var rs = _session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables()
        {
            var expectedValues = new List<object[]>();
            var batch = new BatchStatement();

            CreateTwoTableTestEnv("table1", "table2");

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table1 (id, label, number) VALUES ({0}, '{1}', {2})", 1, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table2 (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            _session.Execute(batch);

            //Verify Results
            RowSet rsTable1 = _session.Execute("SELECT * FROM table1");
            VerifyData(rsTable1, new List<object[]> { new object[] { 1, "label1", 1 } });

            RowSet rsTable2 = _session.Execute("SELECT * FROM table2");
            VerifyData(rsTable2, new List<object[]> { new object[] { 2, "label2", 2 } });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables_OneInvalidTable()
        {
            var batch = new BatchStatement();
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();

            CreateTable(tableName);

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, 1, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table_randomnonexistent (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            Assert.Throws<InvalidQueryException>(() => _session.Execute(batch));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_MixedStatements()
        {
            foreach (var protocolVersion in ProtocolVersionSupported)
            {
                //Use all possible protocol versions
                Cluster.MaxProtocolVersion = protocolVersion;
                //Use a local cluster
                var localCluster = Cluster.Builder().AddContactPoint(_testCluster.ClusterIpPrefix + "1").Build();
                var localSession = localCluster.Connect();
                localSession.CreateKeyspaceIfNotExists("tester");
                localSession.ChangeKeyspace("tester");
                var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
                CreateTable(localSession, tableName);
                TestUtils.WaitForSchemaAgreement(localSession.Cluster);

                var simpleStatement =
                    new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, {2}, {3})", tableName, 1, "label", 2));
                var ps = localSession.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
                var batchStatement = new BatchStatement();
                var expectedValues = new List<object[]> { new object[] { 1, "label", 2 }, new object[] { 1, "test", 2 } };

                batchStatement.Add(ps.Bind(new object[] { 1, "test", 2 }));
                batchStatement.Add(simpleStatement);

                var rs = localSession.Execute("SELECT * FROM " + tableName);
                VerifyData(rs, expectedValues);
            }
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_SerialConsistency()
        {
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            CreateTable(tableName);

            var query = new SimpleStatement(String.Format("INSERT INTO {0} (id) values (-99999)", tableName));

            Assert.Throws<ArgumentException>(() =>
            {
                //You can not specify local serial consistency as a valid read one.
                var batch = new BatchStatement()
                    .Add(query)
                    .SetBatchType(BatchType.Logged)
                    .SetSerialConsistencyLevel(ConsistencyLevel.Quorum);
                _session.Execute(batch);
            });

            //It should work
            var statement = new BatchStatement()
                .Add(query)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);

            //Read consistency specified and write consistency specified
            _session.Execute(statement);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_Timestamp()
        {
            string tableName = TestUtils.GetUniqueTableName().ToLower();
            CreateTable(tableName);

            var query = new SimpleStatement(String.Format("INSERT INTO {0} (id) values (-99999)", tableName));

            Assert.DoesNotThrow(() =>
            {
                //It should work
                var statement = new BatchStatement()
                    .Add(query)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum)
                    .SetTimestamp(DateTime.Now);

                //Read consistency specified and write consistency specified
                _session.Execute(statement);
            });
        }

        [Test]
        [TestCassandraVersion(2, 0, Comparison.Equal)]
        public void Batch_PreparedStatements_FlagsNotSupportedInC2_0()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var ps = _session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            var batch = new BatchStatement();
            batch.Add(ps.Bind(new object[] { 1, "label1", 1 }));
            Assert.Throws<NotSupportedException>(() => _session.Execute(batch.SetTimestamp(DateTime.Now)));
        }

        [Test]
        [TestCassandraVersion(1, 9, Comparison.LessThan)]
        public void BatchPreparedStatementsNotSupportedInC1_2()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var ps = _session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            batch.Add(ps.Bind(new object[] { 1, "label1", 1 }));
            try
            {
                _session.Execute(batch);
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
            string tableName = "table" + Guid.NewGuid().ToString("N");

            CreateTable(tableName);


            PreparedStatement ps = _session.Prepare(String.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            List<object[]> expectedValues = new List<object[]>();

            int numberOfPreparedStatements = 100;
            for (var x = 1; x <= numberOfPreparedStatements; x++)
            {
                expectedValues.Add(new object[] { x, "value" + x, x });
                batch.Add(ps.Bind(new object[] { x, "value" + x, x }));
            }

            _session.Execute(batch);

            // Verify correct values
            RowSet rs = _session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        private static void VerifyData(IEnumerable<Row> rowSet, List<object[]> expectedValues)
        {
            int x = 0;
            foreach (Row row in rowSet)
            {
                int y = 0;
                object[] objArr = expectedValues[x];

                var rowEnum = row.GetEnumerator();
                while (rowEnum.MoveNext())
                {
                    var current = rowEnum.Current;
                    Assert.True(objArr[y].Equals(current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
                    y++;
                }

                x++;
            }
        }

        private void CreateTable(string tableName)
        {
            CreateTable(_session, tableName);
        }

        private void CreateTable(ISession session, string tableName)
        {
            QueryTools.ExecuteSyncNonQuery(session, string.Format(@"CREATE TABLE {0}(
                                                                id int PRIMARY KEY,
                                                                label text,
                                                                number int
                                                                );", tableName));
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }


        private void CreateTwoTableTestEnv(string table1, string table2)
        {
            QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"CREATE TABLE {0} (
                                                                          id int PRIMARY KEY,
                                                                          label text,
                                                                          number int
                                                                          );", table1));

            QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"CREATE TABLE {0} (
                                                                        id int PRIMARY KEY,
                                                                        label text,
                                                                        number int
                                                                        );", table2));
            TestUtils.WaitForSchemaAgreement(_session.Cluster);
        }
    }
}