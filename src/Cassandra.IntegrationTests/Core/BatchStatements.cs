using System;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BatchStatements : SingleNodeClusterTest
    {
        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchPreparedStatementTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            CreateTable(tableName);

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            Session.Execute(batch);

            // Verify results
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchPreparedStatementBasicAsyncTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            CreateTable(tableName);

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }
            var task = Session.ExecuteAsync(batch);
            var rs = task.Result;
            
            // Verify results
            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchSimpleStatementSingle()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();

            CreateTable(tableName);

            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, 1, "label 1", 10));
            batch.Add(simpleStatement);
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute("SELECT * FROM " + tableName);
            var row = rs.First();
            Assert.True(row != null, "There should be a row stored.");
            Assert.True(row.SequenceEqual(new object[] { 1, "label 1", 10 }), "Stored values dont match");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchSimpleStatementSingleBinded()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();

            CreateTable(tableName);

            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            batch.Add(simpleStatement.Bind(100, "label 100", 10000));
            Session.Execute(batch);

            //Verify Results
            var rs = Session.Execute("SELECT * FROM " + tableName);
            var row = rs.First();
            Assert.True(row != null, "There should be a row stored.");
            Assert.True(row.SequenceEqual(new object[] { 100, "label 100", 10000}), "Stored values dont match");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchSimpleStatementMultiple()
        {
            SimpleStatement simpleStatement = null;
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var expectedValues = new List<object[]>();

            CreateTable(tableName);

            BatchStatement batch = new BatchStatement();

            for(var x = 1; x <= 5; x++)
            {
                simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, x, "label" + x, x * x));
                expectedValues.Add(new object[] { x, "label" + x, x * x });
                batch.Add(simpleStatement);
            }
            Session.Execute(batch);

            var rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs.OrderBy(r => r.GetValue<int>("id")), expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchStatementTwoTablesTest()
        {
            var expectedValues = new List<object[]>();
            var batch = new BatchStatement();

            CreateTwoTableTestEnv("table1", "table2");

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table1 (id, label, number) VALUES ({0}, '{1}', {2})", 1, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table2 (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            Session.Execute(batch);

            //Verify Results
            RowSet rsTable1 = Session.Execute("SELECT * FROM table1");
            VerifyData(rsTable1, new List<object[]> {new object[] { 1, "label1", 1 } });

            RowSet rsTable2 = Session.Execute("SELECT * FROM table2");
            VerifyData(rsTable2, new List<object[]> { new object[] { 2, "label2", 2 } });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchStatementOnTwoTablesWithOneInvalidTableTest()
        {
            var batch = new BatchStatement();
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();

            CreateTable(tableName);

            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO {0} (id, label, number) VALUES ({1}, '{2}', {3})", tableName, 1, "label1", 1)));
            batch.Add(new SimpleStatement(String.Format(@"INSERT INTO table2 (id, label, number) VALUES ({0}, '{1}', {2})", 2, "label2", 2)));

            Assert.Throws<InvalidQueryException>(
                delegate { Session.Execute(batch); }, "expected InvalidQueryException, but did not get one");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void BatchMixedStatements()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, label, number) VALUES ({1}, {2}, {3})", tableName, 1, "label", 2));
            var ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            var batchStatement = new BatchStatement();
            var expectedValues = new List<object[]> { new object[] { 1, "label", 2 }, new object[] { 1, "test", 2 } };

            batchStatement.Add(ps.Bind(new object[] { 1, "test", 2 }));
            batchStatement.Add(simpleStatement);

            var rs = Session.Execute("SELECT * FROM " + tableName);
            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(1, 9, Comparison.LessThan)]
        public void BatchPreparedStatementsNotSupportedInC1_2()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            CreateTable(tableName);

            var ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            batch.Add(ps.Bind(new object[] { 1, "label1", 1 }));
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
        public void LargeBatchPreparedStatement()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");

            CreateTable(tableName);
            
            
            PreparedStatement ps = Session.Prepare(String.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            List<object[]> expectedValues = new List<object[]>();
            
            int numberOfPreparedStatements = 100;
            for (var x = 1; x <= numberOfPreparedStatements; x++)
            {
                expectedValues.Add(new object[] { x, "value" + x, x });
                batch.Add(ps.Bind(new object[] { x, "value" + x , x }));
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
                    Assert.True(objArr[y].Equals(current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
                    y++;
                }

                x++;
            }
        }

        private void CreateTable(string tableName)
        {
            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                                                                    id int PRIMARY KEY,
                                                                    label text,
                                                                    number int
                                                                    );", tableName)));
        }

        private void CreateTwoTableTestEnv(string table1, string table2)
        {
            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0} (
                                                                          id int PRIMARY KEY,
                                                                          label text,
                                                                          number int
                                                                          );", table1)));

            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0} (
                                                                          id int PRIMARY KEY,
                                                                          label text,
                                                                          number int
                                                                          );", table2)));
        }
    }
}
