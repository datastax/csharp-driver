using NUnit.Framework;
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
    class ParameterizedStatementsTests
    {
        private ISession Session;

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMBridge.ReusableCCMCluster.Setup(2);
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder());
            Session = CCMBridge.ReusableCCMCluster.Connect("tester");
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMBridge.ReusableCCMCluster.Drop();
        }

        [TestMethod]
        public void TestText()
        {
            ParameterizedStatementTest(typeof(string));
        }

        [TestMethod]
        public void testBlob()
        {
            ParameterizedStatementTest(typeof(byte));
        }

        [TestMethod]
        public void testASCII()
        {
            ParameterizedStatementTest(typeof(Char));
        }

        [TestMethod]
        public void testDecimal()
        {
            ParameterizedStatementTest(typeof(Decimal));
        }

        [TestMethod]
        public void testVarInt()
        {
            ParameterizedStatementTest(typeof(BigInteger));
        }

        [TestMethod]
        public void testBigInt()
        {
            ParameterizedStatementTest(typeof(Int64));
        }

        [TestMethod]
        public void testDouble()
        {
            ParameterizedStatementTest(typeof(Double));
        }

        [TestMethod]
        public void testFloat()
        {
            ParameterizedStatementTest(typeof(Single));
        }

        [TestMethod]
        public void testInt()
        {
            ParameterizedStatementTest(typeof(Int32));
        }

        [TestMethod]
        public void testBoolean()
        {
            ParameterizedStatementTest(typeof(Boolean));
        }

        [TestMethod]
        public void testUUID()
        {
            ParameterizedStatementTest(typeof(Guid));
        }

        [TestMethod]
        public void testTimeStamp()
        {
            ParameterizedStatementTimeStampTest();
        }

        [TestMethod]
        public void testIntAsync()
        {
            ParameterizedStatementTest(typeof(Int32), true);
        }

        private void ParameterizedStatementTimeStampTest(bool testAsync=false)
        {
            RowSet rs = null;
            var expectedValues = new List<object[]>(1);
            var unixtime = new Int64();
            unixtime = 1398095658;
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var valuesToTest = new List<object[]> { new object[] { Guid.NewGuid(), new DateTimeOffset(2011, 2, 3, 16, 5, 0, new TimeSpan(0000)) },
                                                    {new object[] {Guid.NewGuid(), (long)0}},
                                                    { new object[] { Guid.NewGuid(), unixtime }}};
            
            foreach (var bindValues in valuesToTest)
            {
                expectedValues.Add(bindValues);

                CreateTable(tableName, "timestamp");

                SimpleStatement statement = new SimpleStatement(String.Format("INSERT INTO {0} (id, val) VALUES (?, ?)", tableName));
                statement.Bind(bindValues);

                if (testAsync)
                {
                    Session.ExecuteAsync(statement);
                }
                else
                {
                    Session.Execute(statement);
                }

                // Verify results
                rs = Session.Execute("SELECT * FROM " + tableName);

                VerifyData(rs, expectedValues);

                DropTable(tableName);

                expectedValues.Clear();
            }
        }

        private void ParameterizedStatementTest(Type type, bool testAsync = false)
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(type);
            var expectedValues = new List<object[]>(1);
            var val = Randomm.RandomVal(type);
            var bindValues = new object[] { Guid.NewGuid(), val };
            expectedValues.Add(bindValues);
            
            CreateTable(tableName, cassandraDataTypeName);

            SimpleStatement statement = new SimpleStatement(String.Format("INSERT INTO {0} (id, val) VALUES (?, ?)", tableName));
            statement.Bind(bindValues);

            if (testAsync)
            {
                Session.ExecuteAsync(statement);
            }
            else
            {
                Session.Execute(statement);
            }

            // Verify results
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);
            VerifyData(rs, expectedValues);

        }

        private void CreateTable(string tableName, string type)
        {
            try
            {
                Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                                                                        id uuid PRIMARY KEY,
                                                                        val {1}
                                                                        );", tableName, type)));
            }
            catch (AlreadyExistsException)
            {
            }
        }

        private void DropTable(string tableName)
        {
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"DROP TABLE {0};", tableName));
        }

        private static DateTimeOffset FromUnixTime(long unixTime)
        {
            var epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, new TimeSpan(0000));
            return epoch.AddSeconds(unixTime);
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
                    if (objArr[y].GetType() == typeof(byte[]))
                    {
                        Assert.AreEqual((byte[])objArr[y], (byte[])current);
                    }
                    else if (current.GetType() == typeof(DateTimeOffset))
                    {
                        if (objArr[y].GetType() == typeof(long))
                        {
                            if ((long)objArr[y] == 0)
                            {
                                Assert.True(current.ToString() == "1/1/1970 12:00:00 AM +00:00");
                            }
                            else
                            {
                                Assert.AreEqual(FromUnixTime((long)objArr[y]), (DateTimeOffset)current, String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
                            }
                        }
                        else
                        {
                            Assert.AreEqual((DateTimeOffset)objArr[y], ((DateTimeOffset)current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
                        }
                    }
                    else
                    {
                        Assert.True(objArr[y].Equals(current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
                    }
                    y++;
                }

                x++;
            }
        }
    }
}
