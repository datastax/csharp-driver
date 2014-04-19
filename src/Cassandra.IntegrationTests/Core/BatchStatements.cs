using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    class BatchStatements
    {
        private ISession Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder().WithCompression(CompressionType.LZ4));
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        [TestMethod]
        public void BatchPreparedStatementTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            CreateTable(tableName);

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.AddQuery(ps.Bind(val));
            }
            Session.Execute(batch);

            // Verify results
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        [TestMethod]
        public void BatchPreparedStatementAsyncTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            CreateTable(tableName);

            PreparedStatement ps = Session.Prepare(string.Format(@"INSERT INTO {0} (id, label, number) VALUES (?, ?, ?)", tableName));
            BatchStatement batch = new BatchStatement();
            foreach (object[] val in expectedValues)
            {
                batch.AddQuery(ps.Bind(val));
            }
            var task = Session.ExecuteAsync(batch);
            var rs = task.Result;
            
            // Verify results
            VerifyData(rs, expectedValues);
        }

        [TestIgnore]
        [TestMethod]
        public void BatchSimpleStatement()
        {
            string statement = null;
            SimpleStatement simpleStatement = null;
            string tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            List<object[]> expectedValues = new List<object[]>();

            CreateTable(tableName);

            BatchStatement batch = new BatchStatement();

            for(var x = 1; x <= 5; x++)
            {
                simpleStatement = new SimpleStatement(String.Format("INSERT INTO {0}(id, label, number) VALUES ({1}, {2}, {3})", tableName, x, "label" + x, x * x));
                expectedValues.Add(new object[] { x, "label" + x, x * x });
                batch.AddQuery(simpleStatement);
            }
            Session.Execute(batch);

            //Verify Results
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        [TestIgnore]
        [TestMethod]
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
                batch.AddQuery(ps.Bind(new object[] { x, "value" +1 , x }));
            }
            
            Session.Execute(batch);

            // Verify correct values
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        private static void VerifyData(RowSet rowSet, List<object[]> expectedValues)
        {
            int x = 0;
            foreach (Row row in rowSet.GetRows())
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
            try
            {
                Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                                                                        id int PRIMARY KEY,
                                                                        label text,
                                                                        number int
                                                                        );", tableName)));
            }
            catch (AlreadyExistsException)
            {
            }
        }
    }
}
