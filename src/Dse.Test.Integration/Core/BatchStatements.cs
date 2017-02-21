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
﻿using System.Threading;
﻿using Cassandra.IntegrationTests.TestBase;
﻿using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BatchStatements : SharedClusterTest
    {
        private readonly string _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();

        public BatchStatements() : base(3)
        {
            
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            CreateTable(_tableName);
            Thread.Sleep(2000);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement()
        {
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            Session.Execute(batch);

            // Verify results
            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id IN ({1}, {2}, {3})", _tableName, 1, 2, 3));

            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_AsyncTest()
        {
            List<object[]> expectedValues = new List<object[]> { new object[] { 10, "label1", 1 }, new object[] { 11, "label2", 2 }, new object[] { 12, "label3", 3 } };

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
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
            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", _tableName, 20, "label 20", 20));
            batch.Add(simpleStatement);
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id IN ({1})", _tableName, 20));
            var row = rs.First();
            CollectionAssert.AreEqual(row, new object[] { 20, "label 20", 20});
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Bound()
        {
            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            #pragma warning disable 618
            batch.Add(simpleStatement.Bind(100, "label 100", 10000));
            #pragma warning restore 618
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id IN ({1})", _tableName, 100));
            var row = rs.First();
            CollectionAssert.AreEqual(row, new object[] {100, "label 100", 10000});
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Multiple()
        {
            var expectedValues = new List<object[]>();

            BatchStatement batch = new BatchStatement();

            for (var x = 200; x < 205; x++)
            {
                var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", _tableName, x, "label" + x, x * x));
                expectedValues.Add(new object[] { x, "label" + x, x * x });
                batch.Add(simpleStatement);
            }
            Session.Execute(batch);

            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id IN ({1})", _tableName, "200, 201, 202, 203, 204"));

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables()
        {
            var batch = new BatchStatement();

            CreateTwoTableTestEnv("table1", "table2");

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table1 (id, label, number) VALUES ({0}, '{1}', {2})", 1, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table2 (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            Session.Execute(batch);

            //Verify Results
            RowSet rsTable1 = Session.Execute("SELECT * FROM table1");
            VerifyData(rsTable1, new List<object[]> { new object[] { 1, "label1", 1 } });

            RowSet rsTable2 = Session.Execute("SELECT * FROM table2");
            VerifyData(rsTable2, new List<object[]> { new object[] { 2, "label2", 2 } });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables_OneInvalidTable()
        {
            var batch = new BatchStatement();

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", _tableName, 400, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table_randomnonexistent (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            Assert.Throws<InvalidQueryException>(() => Session.Execute(batch));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_MixedStatement()
        {
            var simpleStatement =
                new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, {2}, {3})", _tableName, 500, "label 500", 2));
            var ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            var batchStatement = new BatchStatement();
            var expectedValues = new List<object[]> { new object[] { 500, "label 500", 2 }, new object[] { 501, "test", 2 } };

            batchStatement.Add(ps.Bind(501, "test", 2));
            batchStatement.Add(simpleStatement);

            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id IN ({1}, {2})", _tableName, 500, 501));
            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_SerialConsistency()
        {
            var query = new SimpleStatement(String.Format("INSERT INTO {0} (id) values (-99999)", _tableName));

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
        public void Batch_Timestamp()
        {
            var query = new SimpleStatement(String.Format("INSERT INTO {0} (id) values (-99999)", _tableName));

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
            var ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            var batch = new BatchStatement();
            batch.Add(ps.Bind(1, "label1", 1));
            Assert.Throws<NotSupportedException>(() => Session.Execute(batch.SetTimestamp(DateTime.Now)));
        }

        [Test]
        [TestCassandraVersion(1, 9, Comparison.LessThan)]
        public void Batch_PreparedStatements_NotSupportedInC1_2()
        {
            var ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", _tableName));
            BatchStatement batch = new BatchStatement();
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
            string tableName = "table" + Guid.NewGuid().ToString("N");

            CreateTable(tableName);


            PreparedStatement ps = Session.Prepare(String.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            List<object[]> expectedValues = new List<object[]>();

            for (var x = 1; x <= 100; x++)
            {
                expectedValues.Add(new object[] { x, "value" + x, x });
                batch.Add(ps.Bind(new object[] { x, "value" + x, x }));
            }

            Session.Execute(batch);

            // Verify correct values
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

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
                    Assert.True(objArr[y].Equals(current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y], current));
                    y++;
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
            QueryTools.ExecuteSyncNonQuery(session, string.Format(@"CREATE TABLE {0}(
                                                                id int PRIMARY KEY,
                                                                label text,
                                                                number int
                                                                );", tableName));
            TestUtils.WaitForSchemaAgreement(session.Cluster);
        }


        private void CreateTwoTableTestEnv(string table1, string table2)
        {
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0} (
                                                                          id int PRIMARY KEY,
                                                                          label text,
                                                                          number int
                                                                          );", table1));

            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0} (
                                                                        id int PRIMARY KEY,
                                                                        label text,
                                                                        number int
                                                                        );", table2));
            TestUtils.WaitForSchemaAgreement(Session.Cluster);
        }
    }
}