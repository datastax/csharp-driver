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

using System;
using System.Linq;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using System.Reflection;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BasicTypeTests : SharedClusterTest
    {
        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryBinding()
        {
            string tableName = CreateSimpleTableAndInsert(0);
            var sst = new SimpleStatement(string.Format("INSERT INTO {0}(id, label) VALUES(?, ?)", tableName));
            Session.Execute(sst.Bind(new object[] { Guid.NewGuid(), "label" }));
        }

        /// <summary>
        /// Validates that the Session.GetRequest (called within ExecuteAsync) method uses the session default paging size
        /// which was set previously when the Builder was initialized
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 0)]
        public void PagingOnBoundStatementTest_Async_UsingConfigBasedPagingSetting()
        {
            var pageSize = 10;
            var queryOptions = new QueryOptions().SetPageSize(pageSize);
            Builder builder = new Builder().WithQueryOptions(queryOptions).WithDefaultKeyspace(KeyspaceName);
            builder.AddContactPoint(TestCluster.InitialContactPoint);

            using (ISession session = builder.Build().Connect())
            {
                var totalRowLength = 1003;
                Tuple<string, string> tableNameAndStaticKeyVal = CreateTableWithCompositeIndexAndInsert(session, totalRowLength);
                string statementToBeBound = "SELECT * from " + tableNameAndStaticKeyVal.Item1 + " where label=?";
                PreparedStatement preparedStatementWithoutPaging = session.Prepare(statementToBeBound);
                PreparedStatement preparedStatementWithPaging = session.Prepare(statementToBeBound);
                BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(tableNameAndStaticKeyVal.Item2);
                BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(tableNameAndStaticKeyVal.Item2);

                var rsWithSessionPagingInherited = session.ExecuteAsync(boundStatemetWithPaging).Result;

                var rsWithoutPagingInherited = Session.Execute(boundStatemetWithoutPaging);

                //Check that the internal list of items count is pageSize
                Assert.AreEqual(pageSize, rsWithSessionPagingInherited.InnerQueueCount);
                Assert.AreEqual(totalRowLength, rsWithoutPagingInherited.InnerQueueCount);

                var allTheRowsPaged = rsWithSessionPagingInherited.ToList();
                Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PagingOnBoundStatementTest()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            Tuple<string, string> tableNameAndStaticKeyVal = CreateTableWithCompositeIndexAndInsert(Session, totalRowLength);
            string statementToBeBound = "SELECT * from " + tableNameAndStaticKeyVal.Item1 + " where label=?";
            PreparedStatement preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(tableNameAndStaticKeyVal.Item2);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(tableNameAndStaticKeyVal.Item2);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PagingOnBoundStatementTest_PageOverOneRow()
        {
            var pageSize = 10;
            var totalRowLength = 11;
            string tableName = CreateSimpleTableAndInsert(totalRowLength);

            // insert a guid that we'll keep track of
            Guid guid = Guid.NewGuid();
            Session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", guid, "LABEL_12345", tableName));

            string statementToBeBound = "SELECT * from " + tableName + " where id=?";
            PreparedStatement preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(1, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(1, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(1, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PagingOnBoundStatementTest_PageOverZeroRows()
        {
            var pageSize = 10;
            var totalRowLength = 11;
            string tableName = CreateSimpleTableAndInsert(totalRowLength);

            // insert a guid that we'll keep track of
            Guid guid = Guid.NewGuid();

            string statementToBeBound = "SELECT * from " + tableName + " where id=?";
            PreparedStatement preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(0, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(0, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(0, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void PagingOnSimpleStatementTest()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var statementWithPaging = new SimpleStatement("SELECT * FROM " + table);
            var statementWithoutPaging = new SimpleStatement("SELECT * FROM " + table);
            statementWithoutPaging.SetPageSize(int.MaxValue);
            statementWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(statementWithPaging);
            var rsWithoutPaging = Session.Execute(statementWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.True(rsWithPaging.InnerQueueCount == pageSize);
            Assert.True(rsWithoutPaging.InnerQueueCount == totalRowLength);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.True(allTheRowsPaged.Count == totalRowLength);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryPaging()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var rsWithoutPaging = Session.Execute("SELECT * FROM " + table, int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var rs = Session.Execute("SELECT * FROM " + table, pageSize);
            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rs.InnerQueueCount);

            //Use Linq to iterate through all the rows
            var allTheRowsPaged = rs.ToList();

            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryPagingParallel()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var query = new SimpleStatement(String.Format("SELECT * FROM {0} LIMIT 10000", table))
                .SetPageSize(pageSize);
            var rs = Session.Execute(query);
            Assert.AreEqual(pageSize, rs.GetAvailableWithoutFetching());
            var counterList = new ConcurrentBag<int>();
            Action iterate = () =>
            {
                var counter = rs.Count();
                counterList.Add(counter);
            };

            //Iterate in parallel the RowSet
            Parallel.Invoke(iterate, iterate, iterate, iterate);

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength, counterList.Sum());
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryPagingMultipleTimesOverTheSameStatement()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var times = 10;
            var table = CreateSimpleTableAndInsert(totalRowLength);

            var statement = new SimpleStatement(String.Format("SELECT * FROM {0} LIMIT 10000", table))
                .SetPageSize(pageSize);

            var counter = 0;
            for (var i = 0; i < times; i++)
            {
                var rs = Session.Execute(statement);
                counter += rs.Count();
            }

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryPagingManual()
        {
            var pageSize = 10;
            var totalRowLength = 15;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var rs = Session.Execute(new SimpleStatement("SELECT * FROM " + table).SetAutoPage(false).SetPageSize(pageSize));
            Assert.NotNull(rs.PagingState);
            //It should have just the first page of rows
            Assert.AreEqual(pageSize, rs.InnerQueueCount);
            //Linq iteration should not make it to page
            Assert.AreEqual(pageSize, rs.Count());
            rs = Session.Execute(new SimpleStatement("SELECT * FROM " + table).SetAutoPage(false).SetPageSize(pageSize).SetPagingState(rs.PagingState));
            //It should only contain the following page rows
            Assert.AreEqual(totalRowLength - pageSize, rs.Count());
        }

        [Test]
        public void QueryTraceEnabledTest()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local").EnableTracing());
            Assert.NotNull(rs.Info.QueryTrace);
            Assert.AreEqual(IPAddress.Parse(TestCluster.InitialContactPoint), rs.Info.QueryTrace.Coordinator);
            Assert.Greater(rs.Info.QueryTrace.Events.Count, 0);
            if (Session.BinaryProtocolVersion >= 4)
            {
                Assert.NotNull(rs.Info.QueryTrace.ClientAddress);   
            }
            else
            {
                Assert.Null(rs.Info.QueryTrace.ClientAddress);
            }
        }

        [Test]
        public void QueryTraceDisabledByDefaultTest()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local"));
            Assert.Null(rs.Info.QueryTrace);
        }

        /// Tests that the default consistency level for queries is LOCAL_ONE
        /// 
        /// LocalOne_Is_Default_Consistency tests that the default consistency level for all queries is LOCAL_ONE. It performs
        /// a simple select statement and verifies that the result set metadata shows that the achieved consistency level is LOCAL_ONE.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-378
        /// @expected_result The default consistency level should be LOCAL_ONE
        /// 
        /// @test_category consistency
        [Test]
        public void LocalOne_Is_Default_Consistency()
        {
            var rs = Session.Execute(new SimpleStatement("SELECT * from system.local"));
            Assert.AreEqual(ConsistencyLevel.LocalOne, rs.Info.AchievedConsistency);
        }

        [Test]
        public void Counter()
        {
            TestCounters();
        }

        [Test]
        public void TypeBlob()
        {
            InsertingSingleValue(typeof(byte));
        }

        [Test]
        public void TypeASCII()
        {
            InsertingSingleValue(typeof(Char));
        }

        [Test]
        public void TypeDecimal()
        {
            InsertingSingleValue(typeof(Decimal));
        }

        [Test]
        public void TypeVarInt()
        {
            InsertingSingleValue(typeof(BigInteger));
        }

        [Test]
        public void TypeBigInt()
        {
            InsertingSingleValue(typeof(Int64));
        }

        [Test]
        public void TypeDouble()
        {
            InsertingSingleValue(typeof(Double));
        }

        [Test]
        public void TypeFloat()
        {
            InsertingSingleValue(typeof(Single));
        }

        [Test]
        public void TypeInt()
        {
            InsertingSingleValue(typeof(Int32));
        }

        [Test]
        public void TypeBoolean()
        {
            InsertingSingleValue(typeof(Boolean));
        }

        [Test]
        public void TypeUUID()
        {
            InsertingSingleValue(typeof(Guid));
        }

        [Test]
        public void TypeTimestamp()
        {
            TimestampTest();
        }

        [Test]
        public void TypeInt_Max()
        {
            ExceedingCassandraType(typeof(Int32), typeof(Int32));
        }

        [Test]
        public void TypeBigInt_Max()
        {
            ExceedingCassandraType(typeof(Int64), typeof(Int64));
        }

        [Test]
        public void TypeFloat_Max()
        {
            ExceedingCassandraType(typeof(Single), typeof(Single));
        }

        [Test]
        public void TypeDouble_Max()
        {
            ExceedingCassandraType(typeof(Double), typeof(Double));
        }

        [Test]
        public void ExceedingCassandraInt()
        {
            ExceedingCassandraType(typeof(Int32), typeof(Int64), false);
        }

        [Test]
        public void ExceedingCassandraFloat()
        {
            ExceedingCassandraType(typeof(Single), typeof(Double), false);
        }

        /// <summary>
        /// Test the convertion of a decimal value ( with a negative scale) stored in a column.
        /// 
        /// @jira CSHARP-453 https://datastax-oss.atlassian.net/browse/CSHARP-453
        /// 
        /// </summary>
        [Test]
        public void DecimalWithNegativeScaleTest()
        {
            const string query = "CREATE TABLE decimal_neg_scale(id uuid PRIMARY KEY, value decimal);";
            QueryTools.ExecuteSyncNonQuery(Session, query);

            const string insertQuery = @"INSERT INTO decimal_neg_scale (id, value) VALUES (?, ?)";
            var preparedStatement = Session.Prepare(insertQuery);

            const int scale = -1;
            var scaleBytes = BitConverter.GetBytes(scale);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(scaleBytes);
            var bytes = new byte[scaleBytes.Length + 1];
            Array.Copy(scaleBytes, bytes, scaleBytes.Length);

            bytes[scaleBytes.Length] = 5;

            var firstRowValues = new object[] { Guid.NewGuid(), bytes };
            Session.Execute(preparedStatement.Bind(firstRowValues));

            var row = Session.Execute("SELECT * FROM decimal_neg_scale").First();
            var decValue = row.GetValue<decimal>("value");
            
            Assert.AreEqual(50, decValue);

            const string  dropQuery = "DROP TABLE decimal_neg_scale;";
            QueryTools.ExecuteSyncNonQuery(Session, dropQuery);
        }

        ////////////////////////////////////
        /// Test Helpers
        ////////////////////////////////////

        /// <summary>
        /// Creates a table and inserts a number of records synchronously.
        /// </summary>
        /// <returns>The name of the table</returns>
        private string CreateSimpleTableAndInsert(int rowsInTable)
        {
            string tableName = TestUtils.GetUniqueTableName();
            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"
                CREATE TABLE {0}(
                id uuid PRIMARY KEY,
                label text);", tableName));
            for (int i = 0; i < rowsInTable; i++)
            {
                Session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", Guid.NewGuid(), "LABEL" + i, tableName));
            }

            return tableName;
        }

        /// <summary>
        /// Creates a table with a composite index and inserts a number of records synchronously.
        /// </summary>
        /// <returns>The name of the table</returns>
        private Tuple<string, string> CreateTableWithCompositeIndexAndInsert(ISession session, int rowsInTable)
        {
            string tableName = TestUtils.GetUniqueTableName();
            string staticClusterKeyStr = "staticClusterKeyStr";
            QueryTools.ExecuteSyncNonQuery(session, string.Format(@"
                CREATE TABLE {0} (
                id text,
                label text,
                PRIMARY KEY (label, id));",
                tableName));
            for (int i = 0; i < rowsInTable; i++)
            {
                session.Execute(string.Format("INSERT INTO {2} (label, id) VALUES('{0}','{1}')", staticClusterKeyStr, Guid.NewGuid().ToString(), tableName));
            }
            Tuple<string, string> infoTuple = new Tuple<string, string>(tableName, staticClusterKeyStr);
            return infoTuple;
        }

        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool sameOutput = true)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(toExceed);
            string tableName = TestUtils.GetUniqueTableName();
            var query = String.Format("CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, label text, number {1});", tableName, cassandraDataTypeName);
            QueryTools.ExecuteSyncNonQuery(Session, query);

            object Minimum = toExceedWith.GetTypeInfo().GetField("MinValue").GetValue(this);
            object Maximum = toExceedWith.GetTypeInfo().GetField("MaxValue").GetValue(this);

            var row1 = new object[3] { Guid.NewGuid(), "Minimum", Minimum };
            var row2 = new object[3] { Guid.NewGuid(), "Maximum", Maximum };
            var toInsert_and_Check = new List<object[]>(2) { row1, row2 };

            if (toExceedWith == typeof(Double) || toExceedWith == typeof(Single))
            {
                Minimum = Minimum.GetType().GetTypeInfo().GetMethod("ToString", new[] { typeof(string) }).Invoke(Minimum, new object[1] { "r" });
                Maximum = Maximum.GetType().GetTypeInfo().GetMethod("ToString", new[] { typeof(string) }).Invoke(Maximum, new object[1] { "r" });

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsert_and_Check[0][2] = Single.NegativeInfinity;
                    toInsert_and_Check[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[0][0], toInsert_and_Check[0][1], Minimum), null);
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[1][0], toInsert_and_Check[1][1], Maximum), null);
            }
            catch (InvalidQueryException)
            {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    return;
                }
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert_and_Check);
        }


        public void TestCounters()
        {
            string tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = string.Format("CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, incdec counter);", tableName);
                QueryTools.ExecuteSyncNonQuery(Session, query);
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            Parallel.For(0, 100,
                         i =>
                         {
                             QueryTools.ExecuteSyncNonQuery(Session,
                                                            string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                                                                          tweet_id, (i % 2 == 0 ? "-" : "+") + i));
                         });

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
                                        new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
        }

        public void InsertingSingleValue(Type tp)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            string tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = string.Format(@"CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, value {1});", tableName, cassandraDataTypeName);
                QueryTools.ExecuteSyncNonQuery(Session, query);
            }
            catch (AlreadyExistsException)
            {
            }

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof(string))
                val = "'" + val.ToString().Replace("'", "''") + "'";
            var row1 = new object[2] { Guid.NewGuid(), val };
            toInsert.Add(row1);

            bool isFloatingPoint = false;

            if (row1[1].GetType() == typeof(string) || row1[1].GetType() == typeof(byte[]))
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             row1[1].GetType() == typeof(byte[])
                                                                 ? "0x" + CqlQueryTools.ToHex((byte[])toInsert[0][1])
                                                                 : "'" + toInsert[0][1] + "'"), null);
            // rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double))
                    isFloatingPoint = true;
                QueryTools.ExecuteSyncNonQuery(Session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             !isFloatingPoint
                                                                 ? toInsert[0][1]
                                                                 : toInsert[0][1].GetType()
                                                                                 .GetMethod("ToString", new[] { typeof(string) })
                                                                                 .Invoke(toInsert[0][1], new object[] { "r" })), null);
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), toInsert);
        }

        public void TimestampTest()
        {
            string tableName = TestUtils.GetUniqueTableName();
            var createQuery = string.Format(@"CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, ts timestamp);", tableName);
            QueryTools.ExecuteSyncNonQuery(Session, createQuery);

            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         "2011-02-03 04:05+0000"), null);
            QueryTools.ExecuteSyncNonQuery(Session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         220898707200000), null);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(), 0),
                                           null);

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}
