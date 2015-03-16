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
using System.Collections.Concurrent;
using System.Linq;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using System.Net;
using System.Collections;
using System.Threading;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class PreparedStatementsTests : SharedClusterTest
    {
        private const string AllTypesTableName = "all_types_table_prepared";

        public PreparedStatementsTests() : base(3)
        {
            //A 3 node cluster
        }

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            Session.WaitForSchemaAgreement(Session.Execute(String.Format(TestUtils.CreateTableAllTypes, AllTypesTableName)));
        }

        [Test]
        public void Bound_AllSingleTypesDifferentValues()
        {
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new[] {0}, preparedStatement.RoutingIndexes);
            
            var firstRowValues = new object[] 
            { 
                Guid.NewGuid(), "first", 10, Int64.MaxValue - 1, 1.999F, 32.002D, 1.101010M, 
                new byte[] {255, 255}, true, new DateTimeOffset(new DateTime(2005, 8, 5)), new IPAddress(new byte[] {192, 168, 0, 100})
            };
            var secondRowValues = new object[] 
            { 
                Guid.NewGuid(), "second", 0, 0L, 0F, 0D, 0M, 
                new byte[] {0, 0}, true, new DateTimeOffset(new DateTime(1970, 9, 18)), new IPAddress(new byte[] {0, 0, 0, 0})
            };
            var thirdRowValues = new object[] 
            { 
                Guid.NewGuid(), "third", -100, Int64.MinValue + 1, -150.111F, -5.12342D, -8.101010M, 
                new byte[] {1, 1}, true, new DateTimeOffset(new DateTime(1543, 5, 24)), new IPAddress(new byte[] {255, 128, 12, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
            };

            Session.Execute(preparedStatement.Bind(firstRowValues));
            Session.Execute(preparedStatement.Bind(secondRowValues));
            Session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = String.Format(@"
            SELECT
                id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample
            FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = Session.Execute(selectQuery).ToList();
            //Check that they were inserted and retrieved
            Assert.AreEqual(3, rowList.Count);
            
            //Create a dictionary with the inserted values to compare with the retrieved values
            var insertedValues = new Dictionary<Guid, object[]>()
            {
                {(Guid)firstRowValues[0], firstRowValues},
                {(Guid)secondRowValues[0], secondRowValues},
                {(Guid)thirdRowValues[0], thirdRowValues}
            };

            foreach (var retrievedRow in rowList)
            {
                var inserted = insertedValues[retrievedRow.GetValue<Guid>("id")];
                for (var i = 0; i < inserted.Length; i++ )
                {
                    var insertedValue = inserted[i];
                    var retrievedValue = retrievedRow[i];
                    Assert.AreEqual(insertedValue, retrievedValue);
                }
            }
        }

        [Test]
        public void Bound_AllSingleTypesNullValues()
        {
            const string columns = "id, text_sample, int_sample, bigint_sample, float_sample, double_sample, " +
                                   "decimal_sample, blob_sample, boolean_sample, timestamp_sample, inet_sample";
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                ({1}) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName, columns);

            var preparedStatement = Session.Prepare(insertQuery);
            Assert.AreEqual(columns, String.Join(", ", preparedStatement.Metadata.Columns.Select(c => c.Name)));
            var nullRowValues = new object[] 
            { 
                Guid.NewGuid(), null, null, null, null, null, null, null, null, null, null
            };

            Session.Execute(preparedStatement.Bind(nullRowValues));

            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, nullRowValues[0]));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(1, row.Count(v => v != null));
            Assert.IsTrue(row.Count(v => v == null) > 5, "The rest of the row values must be null");
        }

        [Test]
        public void Bound_CollectionTypes()
        {
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, map_sample, list_sample, set_sample) 
                VALUES (?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new[] { 0 }, preparedStatement.RoutingIndexes);

            var firstRowValues = new object[] 
            { 
                Guid.NewGuid(), 
                new Dictionary<string, string> {{"key1", "value1"}, {"key2", "value2"}},
                new List<string> (new [] {"one", "two", "three", "four", "five"}),
                new List<string> (new [] {"set_1one", "set_2two", "set_3three", "set_4four", "set_5five"})
            };
            var secondRowValues = new object[] 
            { 
                Guid.NewGuid(), 
                new Dictionary<string, string>(),
                new List<string>(),
                new List<string>()
            };
            var thirdRowValues = new object[] 
            { 
                Guid.NewGuid(), 
                null,
                null,
                null
            };

            Session.Execute(preparedStatement.Bind(firstRowValues));
            Session.Execute(preparedStatement.Bind(secondRowValues));
            Session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = String.Format(@"
                SELECT
                    id, map_sample, list_sample, set_sample
                FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = Session.Execute(selectQuery).ToList();
            //Check that they were inserted and retrieved
            Assert.AreEqual(3, rowList.Count);

            //Create a dictionary with the inserted values to compare with the retrieved values
            var insertedValues = new Dictionary<Guid, object[]>()
            {
                {(Guid)firstRowValues[0], firstRowValues},
                {(Guid)secondRowValues[0], secondRowValues},
                {(Guid)thirdRowValues[0], thirdRowValues}
            };

            foreach (var retrievedRow in rowList)
            {
                var inserted = insertedValues[retrievedRow.GetValue<Guid>("id")];
                for (var i = 1; i < inserted.Length; i++)
                {
                    var insertedValue = inserted[i];
                    var retrievedValue = retrievedRow[i];
                    if (retrievedValue == null)
                    {
                        //Empty collections are retrieved as nulls
                        Assert.True(insertedValue == null || ((ICollection)insertedValue).Count == 0);
                        continue;
                    }
                    if (insertedValue != null)
                    {
                        Assert.AreEqual(((ICollection) insertedValue).Count, ((ICollection) retrievedValue).Count);
                    }
                    Assert.AreEqual(insertedValue, retrievedValue);
                }
            }
        }

        [Test]
        public void Prepared_NoParams()
        {
            var preparedStatement = Session.Prepare("SELECT * FROM " + AllTypesTableName);
            //No parameters => no routing indexes
            Assert.Null(preparedStatement.RoutingIndexes);
            //Just check that it works
            var rs = Session.Execute(preparedStatement.Bind());
            Assert.NotNull(rs);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Prepared_SetTimestamp()
        {
            var timestamp = new DateTimeOffset(1999, 12, 31, 1, 2, 3, TimeSpan.Zero);
            var id = Guid.NewGuid();
            var insertStatement = Session.Prepare(String.Format("INSERT INTO {0} (id, text_sample) VALUES (?, ?)", AllTypesTableName));
            Session.Execute(insertStatement.Bind(id, "sample text").SetTimestamp(timestamp));
            var row = Session.Execute(new SimpleStatement(String.Format("SELECT id, text_sample, writetime(text_sample) FROM {0} WHERE id = ?", AllTypesTableName), id)).First();
            Assert.NotNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParamsOrder()
        {
            var query = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(query);
            Assert.Null(preparedStatement.RoutingIndexes);
            Assert.AreEqual(preparedStatement.Metadata.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, my_id", String.Join(", ", preparedStatement.Metadata.Columns.Select(c => c.Name)));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);
            CollectionAssert.AreEqual(new [] {3}, preparedStatement.RoutingIndexes);
            Assert.AreEqual(preparedStatement.Metadata.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, id", String.Join(", ", preparedStatement.Metadata.Columns.Select(c => c.Name)));

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new { my_int = 100, my_bigint = -500L, id = id, my_text = "named params ftw!" }));

            var row = Session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(-500L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("named params ftw!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_Nulls()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new {my_bigint = (long?)null,  my_int = 100, my_id = id}));

            var row = Session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.IsNull(row.GetValue<long?>("bigint_sample"));
            Assert.IsNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_CaseInsensitive()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_TeXt, :my_int, :my_bigint, :id)", AllTypesTableName);
            var preparedStatement = Session.Prepare(insertQuery);
            //The routing key is at position 3
            CollectionAssert.AreEqual(new[] { 3 }, preparedStatement.RoutingIndexes);

            var id = Guid.NewGuid();
            Session.Execute(
                preparedStatement.Bind(
                    new { MY_int = -100, MY_BigInt = 1511L, ID = id, MY_text = "yeah!" }));

            var row = Session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(-100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(1511L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("yeah!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Paging()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.WaitForSchemaAgreement(Session.Execute(String.Format(TestUtils.CreateTableAllTypes, table)));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var rsWithoutPaging = Session.Execute("SELECT * FROM " + table, int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var ps = Session.Prepare("SELECT * FROM " + table);
            var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rs.InnerQueueCount);

            //Use Linq to iterate through all the rows
            var allTheRowsPaged = rs.ToList();

            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Paging_Parallel()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(String.Format(TestUtils.CreateTableAllTypes, table));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }
            var ps = Session.Prepare(String.Format("SELECT * FROM {0} LIMIT 10000", table));
            var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
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
        public void Bound_Paging_MultipleTimesOverTheSameStatement()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var times = 10;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(String.Format(TestUtils.CreateTableAllTypes, table));
            for (var i = 0; i < totalRowLength; i++)
            {
                Session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var ps = Session.Prepare(String.Format("SELECT * FROM {0} LIMIT 10000", table));

            var counter = 0;
            for (var i = 0; i < times; i++)
            {
                var rs = Session.Execute(ps.Bind().SetPageSize(pageSize));
                Assert.AreEqual(pageSize, rs.InnerQueueCount);
                counter += rs.Count();
            }

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_Manual_Paging()
        {
            const int pageSize = 15;
            const int totalRowLength = 20;
            var table = "tbl" + Guid.NewGuid().ToString("N").ToLower();
            Session.Execute(String.Format(TestUtils.CreateTableAllTypes, table));
            var insertPs = Session.Prepare(String.Format("INSERT INTO {0} (id) VALUES (?)", table));
            //Insert the rows
            TestHelper.Invoke(() => Session.Execute(insertPs.Bind(Guid.NewGuid())), totalRowLength);

            var ps = Session.Prepare(String.Format("SELECT * FROM {0} LIMIT 10000", table));
            var rs = Session.Execute(ps.Bind().SetAutoPage(false).SetPageSize(pageSize));
            Assert.False(rs.AutoPage);
            Assert.NotNull(rs.PagingState);
            //Dequeue all via Linq
            var ids = rs.Select(r => r.GetValue<Guid>("id")).ToList();
            Assert.AreEqual(pageSize, ids.Count);
            //Retrieve the next page
            var rs2 = Session.Execute(ps.Bind().SetAutoPage(false).SetPagingState(rs.PagingState));
            Assert.Null(rs2.PagingState);
            var ids2 = rs2.Select(r => r.GetValue<Guid>("id")).ToList();
            Assert.AreEqual(totalRowLength - pageSize, ids2.Count);
            Assert.AreEqual(totalRowLength, ids.Union(ids2).Count());
        }

        [Test]
        public void Bound_Int_Valids()
        {
            var psInt32 = Session.Prepare(String.Format("INSERT INTO {0} (id, int_sample) VALUES (?, ?)", AllTypesTableName));

            //Int: only int and blob valid
            AssertValid(Session, psInt32, 100);
            AssertValid(Session, psInt32, new byte[] { 0, 0, 0, 1 });
        }

        [Test]
        public void Bound_Double_Valids()
        {
            var psDouble = Session.Prepare(String.Format("INSERT INTO {0} (id, double_sample) VALUES (?, ?)", AllTypesTableName));

            //Double: Only doubles, longs and blobs (8 bytes)
            AssertValid(Session, psDouble, 1D);
            AssertValid(Session, psDouble, 1L);
            AssertValid(Session, psDouble, new byte[8]);
        }

        [Test]
        public void Bound_Decimal_Valids()
        {
            var psDecimal = Session.Prepare(String.Format("INSERT INTO {0} (id, decimal_sample) VALUES (?, ?)", AllTypesTableName));

            //Decimal: There is type conversion, all numeric types are valid
            AssertValid(Session, psDecimal, 1L);
            AssertValid(Session, psDecimal, 1F);
            AssertValid(Session, psDecimal, 1D);
            AssertValid(Session, psDecimal, 1);
            AssertValid(Session, psDecimal, new byte[16]);
            AssertValid(Session, psDecimal, "some string");
        }

        [Test]
        public void Bound_Collections_List_Valids()
        {
            var session = GetNewSession(KeyspaceName);
            PreparedStatement psList = session.Prepare(String.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)", AllTypesTableName));

            // Valid cases -- NOTE: Only types List and blob are valid
            AssertValid(session, psList, new List<string>(new[] { "one", "two", "three" })); // parameter type = List<string>
            AssertValid(session, psList, new List<string>(new[] { "one", "two" }).Select(s => s)); // parameter type = IEnumerable
            // parameter type = long fails for C* 2.0.x, passes for C* 2.1.x
            // AssertValid(Session, psList, 123456789L);  
        }

        [Test]
        public void Bound_Collections_Map_Valid()
        {
            var session = GetNewSession(KeyspaceName);
            PreparedStatement psMap = session.Prepare(String.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)", AllTypesTableName));
            AssertValid(session, psMap, new Dictionary<string, string> { { "one", "1" }, { "two", "2" } });
        }

        [Test]
        public void Bound_ExtraParameter()
        {
            var session = GetNewSession(KeyspaceName);
            var ps = session.Prepare(String.Format("INSERT INTO {0} (id, list_sample, int_sample) VALUES (?, ?, ?)", AllTypesTableName));
            Assert.Throws(Is
                .InstanceOf<ArgumentException>().Or
                .InstanceOf<InvalidQueryException>().Or
                .InstanceOf<ServerErrorException>(),
                () => session.Execute(ps.Bind(Guid.NewGuid(), null, null, "yeah, this is extra")));
        }

        [Test, Timeout(180000)]
        public void Bound_With_ChangingKeyspace()
        {
            using (var localCluster = Cluster.Builder()
                .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(15000))
                .AddContactPoint(TestCluster.InitialContactPoint)
                .Build())
            {
                var session = localCluster.Connect("system");
                session.Execute("CREATE KEYSPACE bound_changeks_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
                TestUtils.WaitForSchemaAgreement(localCluster);
                var ps = session.Prepare("SELECT * FROM system.local");
                session.ChangeKeyspace("bound_changeks_test");
                Assert.DoesNotThrow(() => TestHelper.Invoke(() => session.Execute(ps.Bind()), 10));
            }
        }

        [Test]
        public void Bound_With_Named_Parameters_Routing_Key()
        {
            Func<string, string, byte[]> calculateKey = (id1, id2) =>
            {
                return new byte[0]
                .Concat(new byte[] { 0, (byte)id1.Length })
                .Concat(System.Text.Encoding.UTF8.GetBytes(id1))
                .Concat(new byte[] { 0 })
                .Concat(new byte[] { 0, (byte)id2.Length })
                .Concat(System.Text.Encoding.UTF8.GetBytes(id2))
                .Concat(new byte[] { 0 }).ToArray();
            };
            Session.Execute("CREATE TABLE tbl_ps_multiple_pk_named (a uuid, b text, c text, d text, primary key ((a, b), c))");
            Thread.Sleep(3000);
            var ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk_named WHERE a = :a AND b = :b AND c = :ce");
            //Parameters at position 1 and 0 are part of the routing key
            CollectionAssert.AreEqual(new[] { 0, 1 }, ps.RoutingIndexes);
            var anon = new {ce = "hello ce2", a = "aValue1", b = "bValue1"};
            var statement = ps.Bind(anon);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon.a, anon.b), statement.RoutingKey.RawRoutingKey); 
            //Now with another parameters
            anon = new { ce = "hello ce2", a = "aValue2", b = "bValue2" };
            statement = ps.Bind(anon);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon.a, anon.b), statement.RoutingKey.RawRoutingKey);

            //With another query, named parameters are different
            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk_named WHERE b = :nice_name_b AND a = :nice_name_a AND c = :nice_name_c");
            //Parameters names are different from partition keys
            Assert.Null(ps.RoutingIndexes);
            ps.SetRoutingNames("nice_name_a", "nice_name_b");
            var anon2 = new { nice_name_b = "b", nice_name_a = "a", nice_name_c = "c" };
            statement = ps.Bind(anon2);
            Assert.NotNull(statement.RoutingKey);
            CollectionAssert.AreEqual(calculateKey(anon2.nice_name_a, anon2.nice_name_b), statement.RoutingKey.RawRoutingKey);
        }

        [Test]
        public void Prepared_With_Composite_Routing_Key()
        {
            Session.Execute("CREATE TABLE tbl_ps_multiple_pk (a uuid, b text, c text, d text, primary key ((a, b), c))");
            var ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = ? AND c = ? AND a = ?");
            //Parameters at position 2 and 0 are part of the routing key
            CollectionAssert.AreEqual(new[] { 2, 0 }, ps.RoutingIndexes);

            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = :b AND a = :a AND c = :ce");
            //Parameters at position 1 and 0 are part of the routing key
            CollectionAssert.AreEqual(new[] { 1, 0 }, ps.RoutingIndexes);
            Assert.NotNull(ps.Bind("a", Guid.NewGuid()).RoutingKey);

            ps = Session.Prepare("SELECT * FROM tbl_ps_multiple_pk WHERE b = :nice_name1 AND a = :nice_name2 AND c = :nice_name3");
            //Parameters names are different from partition keys
            Assert.Null(ps.RoutingIndexes);
        }

        [Test]
        public void Prepared_SelectOne()
        {
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"
                    CREATE TABLE {0}(
                    tweet_id int PRIMARY KEY,
                    numb double,
                    label text);", tableName));
                TestUtils.WaitForSchemaAgreement(Session.Cluster);
            }
            catch (AlreadyExistsException)
            {
            }

            for (int i = 0; i < 10; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (tweet_id, numb, label) VALUES({1}, 0.01,'{2}')", tableName, i, "row" + i));
            }

            var prepSelect = QueryTools.PrepareQuery(Session, string.Format("SELECT * FROM {0} WHERE tweet_id = ?;", tableName));

            var rowId = 5;
            var result = QueryTools.ExecutePreparedSelectQuery(Session, prepSelect, new object[] { rowId });
            foreach (var row in result)
            {
                Assert.True((string)row.GetValue(typeof(int), "label") == "row" + rowId);
            }
            Assert.True(result.Columns != null);
            Assert.True(result.Columns.Length == 3);
        }

        [Test]
        public void Prepared_IpAddress()
        {
            InsertingSingleValuePrepared(typeof(IPAddress));
        }

        /// <summary>
        /// Verify that a prepared statement can be re-bound after a cluster is restarted, using client's "use keyspace" method
        /// </summary>
        [Test]
        public void RePrepareAfterNodeRestart()
        {
            ReprepareTest(true);
        }

        /// <summary>
        /// Verify that a prepared statement can be re-bound after a cluster is restarted, not using client's "use keyspace" method
        /// </summary>
        [Test]
        public void RePrepareAfterNodeRestart_NoUseKeyspace()
        {
            ReprepareTest(false);
        }

        //////////////////////////////
        // Test Helpers
        //////////////////////////////

        private void AssertValid(ISession session, PreparedStatement ps, object value)
        {
            try
            {
                session.Execute(ps.Bind(Guid.NewGuid(), value));
            }
            catch (Exception e)
            {
                string assertFailMsg = string.Format("Exception was thrown, but shouldn't have been! \nException message: {0}, Exception StackTrace: {1}", e.Message, e.StackTrace);
                Assert.Fail(assertFailMsg);
            }
        }

        private void ReprepareTest(bool useKeyspace)
        {
            string keyspace = DefaultKeyspaceName;
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(1);
            testCluster.InitClient(); // make sure client session was just created
            var nonShareableSession = testCluster.Session;

            string fqKeyspaceName = "";

            if (useKeyspace)
                nonShareableSession.ChangeKeyspace(keyspace);
            else
                fqKeyspaceName = keyspace + ".";

            try
            {
                nonShareableSession.Execute("CREATE TABLE " + fqKeyspaceName + "test(k text PRIMARY KEY, i int)");
            }
            catch (AlreadyExistsException)
            {
            }
            nonShareableSession.Execute("INSERT INTO " + fqKeyspaceName + "test (k, i) VALUES ('123', 17)");
            nonShareableSession.Execute("INSERT INTO " + fqKeyspaceName + "test (k, i) VALUES ('124', 18)");

            PreparedStatement ps = nonShareableSession.Prepare("SELECT * FROM " + fqKeyspaceName + "test WHERE k = ?");

            var rs = nonShareableSession.Execute(ps.Bind("123"));
            Assert.AreEqual(rs.First().GetValue<int>("i"), 17);

            testCluster.Stop(1);
            TestUtils.WaitForDown(testCluster.ClusterIpPrefix + "1", testCluster.Cluster, 40);

            testCluster.Start(1);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 60);

            Assert.True(nonShareableSession.Cluster.AllHosts().Select(h => h.IsUp).Any(), "There should be one node up");
            for (var i = 0; i < 10; i++)
            {
                var rowset = nonShareableSession.Execute(ps.Bind("124"));
                Assert.AreEqual(rowset.First().GetValue<int>("i"), 18);
            }
        }


        public void InsertingSingleValuePrepared(Type tp, object value = null)
        {
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            var tableName = "table" + Guid.NewGuid().ToString("N");

            QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                tweet_id uuid PRIMARY KEY,
                value {1}
                );", tableName, cassandraDataTypeName));

            TestUtils.WaitForSchemaAgreement(Session.Cluster);

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof (string))
                val = "'" + val.ToString().Replace("'", "''") + "'";

            var row1 = new [] {Guid.NewGuid(), val};

            toInsert.Add(row1);

            var prep = QueryTools.PrepareQuery(Session,
                                                             string.Format("INSERT INTO {0}(tweet_id, value) VALUES ({1}, ?);", tableName,
                                                                           toInsert[0][0]));
            if (value == null)
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new [] { toInsert[0][1] });
            }
            else
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new [] {value});
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert);
        }
    }
}
