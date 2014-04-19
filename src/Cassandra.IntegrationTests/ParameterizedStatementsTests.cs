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
            CCMBridge.ReusableCCMCluster.Build(Cluster.Builder().WithCompression(CompressionType.LZ4));
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

        private void ParameterizedStatementTimeStampTest()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var bindValues = new object[] { Guid.NewGuid(), 129670590000000 };
            var expectedValues = new List<object[]>(1);
            expectedValues.Add(bindValues);

            CreateTable(tableName, "timestamp");

            SimpleStatement statement = new SimpleStatement(String.Format("INSERT INTO {0} (id, val) VALUES (?, ?)", tableName));
            statement.Bind(bindValues);

            Session.Execute(statement);

            // Verify results
            RowSet rs = Session.Execute("SELECT * FROM " + tableName);

            VerifyData(rs, expectedValues);
        }

        private void ParameterizedStatementTest(Type type)
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

            Session.Execute(statement);

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

        private static DateTime FromUnixTime(long unixTime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
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
                        Assert.ArrEqual<byte>((byte[])objArr[y], (byte[])current);
                    }
                    else if (current.GetType() == typeof(DateTimeOffset))
                    {
                        Assert.True(FromUnixTime((long)objArr[y]).Equals(current), String.Format("Found difference between expected and actual row {0} != {1}", objArr[y].ToString(), current.ToString()));
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
