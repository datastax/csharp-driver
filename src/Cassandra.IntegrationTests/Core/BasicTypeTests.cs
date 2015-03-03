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
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class BasicTypeTests : TestGlobals
    {
        ISession _session = null;
        ITestCluster _testCluster = null;

        [SetUp]
        public void SetupTest()
        {
            if (_session == null)
            {
                _testCluster = TestClusterManager.GetTestCluster(1);
                _session = _testCluster.Session;
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryBinding()
        {
            string tableName = CreateSimpleTableAndInsert(0);
            var sst = new SimpleStatement(string.Format("INSERT INTO {0}(id, label) VALUES(?, ?)", tableName));
            _session.Execute(sst.Bind(new object[] { Guid.NewGuid(), "label" }));
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
            Builder builder = new Builder().WithQueryOptions(queryOptions).WithDefaultKeyspace(DefaultKeyspaceName);
            builder.AddContactPoint(_testCluster.InitialContactPoint);

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

                var rsWithoutPagingInherited = _session.Execute(boundStatemetWithoutPaging);

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
            Tuple<string, string> tableNameAndStaticKeyVal = CreateTableWithCompositeIndexAndInsert((Session)_session, totalRowLength);
            string statementToBeBound = "SELECT * from " + tableNameAndStaticKeyVal.Item1 + " where label=?";
            PreparedStatement preparedStatementWithoutPaging = _session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = _session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(tableNameAndStaticKeyVal.Item2);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(tableNameAndStaticKeyVal.Item2);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = _session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = _session.Execute(boundStatemetWithoutPaging);

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
            _session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", guid, "LABEL_12345", tableName));

            string statementToBeBound = "SELECT * from " + tableName + " where id=?";
            PreparedStatement preparedStatementWithoutPaging = _session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = _session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = _session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = _session.Execute(boundStatemetWithoutPaging);

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
            PreparedStatement preparedStatementWithoutPaging = _session.Prepare(statementToBeBound);
            PreparedStatement preparedStatementWithPaging = _session.Prepare(statementToBeBound);
            BoundStatement boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            BoundStatement boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = _session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = _session.Execute(boundStatemetWithoutPaging);

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

            var rsWithPaging = _session.Execute(statementWithPaging);
            var rsWithoutPaging = _session.Execute(statementWithoutPaging);

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
            var rsWithoutPaging = _session.Execute("SELECT * FROM " + table, int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var rs = _session.Execute("SELECT * FROM " + table, pageSize);
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
            var rs = _session.Execute(query);
            Assert.AreEqual(pageSize, rs.GetAvailableWithoutFetching());
            var counterList = new ConcurrentBag<int>();
            Action iterate = () =>
            {
                var counter = 0;
                foreach (var row in rs)
                {
                    counter++;
                }
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
                var rs = _session.Execute(statement);
                counter += rs.Count();
            }

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        public void LargeBatchInsert_MultipleTypes()
        {
            BigInsertTest(1000);
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
            QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"
                CREATE TABLE {0}(
                id uuid PRIMARY KEY,
                label text);", tableName));
            for (int i = 0; i < rowsInTable; i++)
            {
                _session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", Guid.NewGuid(), "LABEL" + i, tableName));
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
            QueryTools.ExecuteSyncNonQuery(_session, query);

            object Minimum = toExceedWith.GetField("MinValue").GetValue(this);
            object Maximum = toExceedWith.GetField("MaxValue").GetValue(this);

            var row1 = new object[3] { Guid.NewGuid(), "Minimum", Minimum };
            var row2 = new object[3] { Guid.NewGuid(), "Maximum", Maximum };
            var toInsert_and_Check = new List<object[]>(2) { row1, row2 };

            if (toExceedWith == typeof(Double) || toExceedWith == typeof(Single))
            {
                Minimum = Minimum.GetType().GetMethod("ToString", new[] { typeof(string) }).Invoke(Minimum, new object[1] { "r" });
                Maximum = Maximum.GetType().GetMethod("ToString", new[] { typeof(string) }).Invoke(Maximum, new object[1] { "r" });

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsert_and_Check[0][2] = Single.NegativeInfinity;
                    toInsert_and_Check[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(_session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[0][0], toInsert_and_Check[0][1], Minimum), null);
                QueryTools.ExecuteSyncNonQuery(_session,
                                               string.Format("INSERT INTO {0}(tweet_id, label, number) VALUES ({1}, '{2}', {3});", tableName,
                                                             toInsert_and_Check[1][0], toInsert_and_Check[1][1], Maximum), null);
            }
            catch (InvalidQueryException)
            {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
                    Assert.True(true);
                    return;
                }
            }

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert_and_Check);
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }


        public void TestCounters()
        {
            string tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = string.Format("CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, incdec counter);", tableName);
                QueryTools.ExecuteSyncNonQuery(_session, query);
            }
            catch (AlreadyExistsException)
            {
            }

            Guid tweet_id = Guid.NewGuid();

            Parallel.For(0, 100,
                         i =>
                         {
                             QueryTools.ExecuteSyncNonQuery(_session,
                                                            string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                                                                          tweet_id, (i % 2 == 0 ? "-" : "+") + i));
                         });

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName),
                                        _session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
                                        new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }

        public void InsertingSingleValue(Type tp)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            string tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = string.Format(@"CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, value {1});", tableName, cassandraDataTypeName);
                QueryTools.ExecuteSyncNonQuery(_session, query);
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
                QueryTools.ExecuteSyncNonQuery(_session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             row1[1].GetType() == typeof(byte[])
                                                                 ? "0x" + CqlQueryTools.ToHex((byte[])toInsert[0][1])
                                                                 : "'" + toInsert[0][1] + "'"), null);
            // rndm.GetType().GetMethod("Next" + tp.Name).Invoke(rndm, new object[] { })
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double))
                    isFloatingPoint = true;
                QueryTools.ExecuteSyncNonQuery(_session,
                                               string.Format("INSERT INTO {0}(tweet_id,value) VALUES ({1}, {2});", tableName, toInsert[0][0],
                                                             !isFloatingPoint
                                                                 ? toInsert[0][1]
                                                                 : toInsert[0][1].GetType()
                                                                                 .GetMethod("ToString", new[] { typeof(string) })
                                                                                 .Invoke(toInsert[0][1], new object[] { "r" })), null);
            }

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName),
                                        _session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), toInsert);
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }

        public void TimestampTest()
        {
            string tableName = TestUtils.GetUniqueTableName();
            var createQuery = string.Format(@"CREATE TABLE {0}(tweet_id uuid PRIMARY KEY, ts timestamp);", tableName);
            QueryTools.ExecuteSyncNonQuery(_session, createQuery);

            QueryTools.ExecuteSyncNonQuery(_session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         "2011-02-03 04:05+0000"), null);
            QueryTools.ExecuteSyncNonQuery(_session,
                                           string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(),
                                                         220898707200000), null);
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("INSERT INTO {0}(tweet_id,ts) VALUES ({1}, '{2}');", tableName, Guid.NewGuid(), 0),
                                           null);

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName),
                                        _session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }

        public void BigInsertTest(int RowsNo = 5000)
        {
            string tableName = TestUtils.GetUniqueTableName();
            try
            {
                QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"CREATE TABLE {0}(
                     tweet_id uuid,
                     author text,
                     body text,
                     isok boolean,
		             fval float,
		             dval double,
                     PRIMARY KEY(tweet_id))", tableName));
            }
            catch (AlreadyExistsException)
            {
            }

            var longQ = new StringBuilder();
            longQ.AppendLine("BEGIN BATCH ");

            for (int i = 0; i < RowsNo; i++)
            {
                longQ.AppendFormat(@"INSERT INTO {0} (
                            tweet_id, author, isok, body, fval, dval)
                    VALUES ({1},'test{2}',{3},'body{2}',{4},{5});",
                    tableName, Guid.NewGuid(), i, i % 2 == 0 ? "false" : "true", Randomm.Instance.NextSingle(), Randomm.Instance.NextDouble());
            }
            longQ.AppendLine("APPLY BATCH;");
            QueryTools.ExecuteSyncNonQuery(_session, longQ.ToString(), "Inserting...");
            QueryTools.ExecuteSyncQuery(_session, string.Format(@"SELECT * from {0};", tableName),
                                        _session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
            QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"DROP TABLE {0};", tableName));
        }
    }
}
