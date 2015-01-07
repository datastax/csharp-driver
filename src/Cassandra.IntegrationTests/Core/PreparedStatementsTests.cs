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
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class PreparedStatementsTests : TestGlobals
    {
        private const string AllTypesTableName = "all_types_table_prepared";
        ISession _session = null;
        ITestCluster _testCluster = null;
        private string _keyspace;

        [SetUp]
        public void SetupTest()
        {
            _testCluster = TestClusterManager.GetTestCluster(3);
            _keyspace = _testCluster.DefaultKeyspace;
            _session = _testCluster.Session;
            try
            {
                _session.WaitForSchemaAgreement(_session.Execute(String.Format(TestUtils.CreateTableAllTypes, AllTypesTableName)));
            }
            catch (AlreadyExistsException) { }
        }

        [Test]
        public void Bound_AllSingleTypesDifferentValues()
        {
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = _session.Prepare(insertQuery);
            
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

            _session.Execute(preparedStatement.Bind(firstRowValues));
            _session.Execute(preparedStatement.Bind(secondRowValues));
            _session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = String.Format(@"
            SELECT
                id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample
            FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = _session.Execute(selectQuery).ToList();
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
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = _session.Prepare(insertQuery);

            var nullRowValues = new object[] 
            { 
                Guid.NewGuid(), null, null, null, null, null, null, null, null, null, null
            };

            _session.Execute(preparedStatement.Bind(nullRowValues));

            var rs = _session.Execute(String.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, nullRowValues[0]));
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

            var preparedStatement = _session.Prepare(insertQuery);

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

            _session.Execute(preparedStatement.Bind(firstRowValues));
            _session.Execute(preparedStatement.Bind(secondRowValues));
            _session.Execute(preparedStatement.Bind(thirdRowValues));

            var selectQuery = String.Format(@"
                SELECT
                    id, map_sample, list_sample, set_sample
                FROM {0} WHERE id IN ({1}, {2}, {3})", AllTypesTableName, firstRowValues[0], secondRowValues[0], thirdRowValues[0]);
            var rowList = _session.Execute(selectQuery).ToList();
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
            var preparedStatement = _session.Prepare("SELECT * FROM " + AllTypesTableName);
            var rs = _session.Execute(preparedStatement.Bind());
            //Just check that it works
            Assert.NotNull(rs);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Prepared_SetTimestamp()
        {
            var timestamp = new DateTimeOffset(1999, 12, 31, 1, 2, 3, TimeSpan.Zero);
            var id = Guid.NewGuid();
            var insertStatement = _session.Prepare(String.Format("INSERT INTO {0} (id, text_sample) VALUES (?, ?)", AllTypesTableName));
            _session.Execute(insertStatement.Bind(id, "sample text").SetTimestamp(timestamp));
            var row = _session.Execute(new SimpleStatement(String.Format("SELECT id, text_sample, writetime(text_sample) FROM {0} WHERE id = ?", AllTypesTableName)).Bind(id)).First();
            Assert.NotNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParamsOrder()
        {
            var query = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = _session.Prepare(query);
            Assert.AreEqual(preparedStatement.Metadata.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, my_id", String.Join(", ", preparedStatement.Metadata.Columns.Select(c => c.Name)));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = _session.Prepare(insertQuery);
            Assert.AreEqual(preparedStatement.Metadata.Columns.Length, 4);
            Assert.AreEqual("my_text, my_int, my_bigint, my_id", String.Join(", ", preparedStatement.Metadata.Columns.Select(c => c.Name)));

            var id = Guid.NewGuid();
            _session.Execute(
                preparedStatement.Bind(
                    new { my_int = 100, my_bigint = -500L, my_id = id, my_text = "named params ftw!" }));

            var row = _session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(-500L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("named params ftw!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_Nulls()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = _session.Prepare(insertQuery);

            var id = Guid.NewGuid();
            _session.Execute(
                preparedStatement.Bind(
                    new {my_bigint = (long?)null,  my_int = 100, my_id = id}));

            var row = _session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.IsNull(row.GetValue<long?>("bigint_sample"));
            Assert.IsNull(row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Bound_NamedParameters_CaseInsensitive()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_TeXt, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var preparedStatement = _session.Prepare(insertQuery);

            var id = Guid.NewGuid();
            _session.Execute(
                preparedStatement.Bind(
                    new { MY_int = -100, MY_BigInt = 1511L, MY_id = id, MY_text = "yeah!" }));

            var row = _session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

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
            _session.WaitForSchemaAgreement(_session.Execute(String.Format(TestUtils.CreateTableAllTypes, table)));
            for (var i = 0; i < totalRowLength; i++)
            {
                _session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var rsWithoutPaging = _session.Execute("SELECT * FROM " + table, int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var ps = _session.Prepare("SELECT * FROM " + table);
            var rs = _session.Execute(ps.Bind().SetPageSize(pageSize));
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
            _session.WaitForSchemaAgreement(_session.Execute(String.Format(TestUtils.CreateTableAllTypes, table)));
            for (var i = 0; i < totalRowLength; i++)
            {
                _session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }
            var ps = _session.Prepare(String.Format("SELECT * FROM {0} LIMIT 10000", table));
            var rs = _session.Execute(ps.Bind().SetPageSize(pageSize));
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
        public void Bound_Paging_MultipleTimesOverTheSameStatement()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var times = 10;
            var table = "table" + Guid.NewGuid().ToString("N").ToLower();
            _session.WaitForSchemaAgreement(_session.Execute(String.Format(TestUtils.CreateTableAllTypes, table)));
            for (var i = 0; i < totalRowLength; i++)
            {
                _session.Execute(String.Format("INSERT INTO {0} (id, text_sample) VALUES ({1}, '{2}')", table, Guid.NewGuid(), "value" + i));
            }

            var ps = _session.Prepare(String.Format("SELECT * FROM {0} LIMIT 10000", table));

            var counter = 0;
            for (var i = 0; i < times; i++)
            {
                var rs = _session.Execute(ps.Bind().SetPageSize(pageSize));
                Assert.AreEqual(pageSize, rs.InnerQueueCount);
                counter += rs.Count();
            }

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        public void Bound_Int_Valids()
        {
            var psInt32 = _session.Prepare(String.Format("INSERT INTO {0} (id, int_sample) VALUES (?, ?)", AllTypesTableName));

            //Int: only int and blob valid
            AssertValid(_session, psInt32, 100);
            AssertValid(_session, psInt32, new byte[] { 0, 0, 0, 1 });
        }

        [Test]
        public void Bound_Int_Invalids()
        {
            var psInt32 = _session.Prepare(String.Format("INSERT INTO {0} (id, int_sample) VALUES (?, ?)", AllTypesTableName));

            //Int: only int and blob valid
            AssertExceptionTypeIsThrown(_session, psInt32, (short)1, new string[] { "InvalidTypeException" });
            AssertExceptionTypeIsThrown(_session, psInt32, 1D, new string[] { "InvalidTypeException"});
            AssertExceptionTypeIsThrown(_session, psInt32, 1L, new string[] { "InvalidTypeException"});
            AssertExceptionTypeIsThrown(_session, psInt32, new byte[5], new string[] { "InvalidQueryException"});
        }

        [Test]
        public void Bound_Double_Valids()
        {
            var psDouble = _session.Prepare(String.Format("INSERT INTO {0} (id, double_sample) VALUES (?, ?)", AllTypesTableName));

            //Double: Only doubles, longs and blobs (8 bytes)
            AssertValid(_session, psDouble, 1D);
            AssertValid(_session, psDouble, 1L);
            AssertValid(_session, psDouble, new byte[8]);
        }

        [Test, Timeout(120000)]
        public void Bound_Double_Invalids()
        {
            using (var localCluster = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var session = localCluster.Connect(_keyspace);
                var psDouble = session.Prepare(String.Format("INSERT INTO {0} (id, double_sample) VALUES (?, ?)", AllTypesTableName));

                //Double: Only doubles, longs and blobs (8 bytes)
                AssertExceptionTypeIsThrown(session, psDouble, (short)1, new string[] { "InvalidTypeException" });
                AssertExceptionTypeIsThrown(session, psDouble, 1F, new string[] { "InvalidTypeException" });
                AssertExceptionTypeIsThrown(session, psDouble, 100, new string[] { "InvalidTypeException" });
                AssertExceptionTypeIsThrown(session, psDouble, (short)100, new string[] { "InvalidTypeException" });   
            }
        }

        [Test]
        public void Bound_Decimal_Valids()
        {
            var psDecimal = _session.Prepare(String.Format("INSERT INTO {0} (id, decimal_sample) VALUES (?, ?)", AllTypesTableName));

            //Decimal: There is type conversion, all numeric types are valid
            AssertValid(_session, psDecimal, 1L);
            AssertValid(_session, psDecimal, 1F);
            AssertValid(_session, psDecimal, 1D);
            AssertValid(_session, psDecimal, 1);
            AssertValid(_session, psDecimal, new byte[16]);
            AssertValid(_session, psDecimal, "some string");
        }

        [Test]
        public void Bound_Collections_List_Valids()
        {
            PreparedStatement psList = _session.Prepare(String.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)", AllTypesTableName));

            // Valid cases -- NOTE: Only types List and blob are valid
            AssertValid(_session, psList, new List<string>(new[] { "one", "two", "three" })); // parameter type = List<string>
            AssertValid(_session, psList, new List<string>(new[] { "one", "two" }).Select(s => s)); // parameter type = IEnumerable
            // parameter type = long fails for C* 2.0.x, passes for C* 2.1.x
            // AssertValid(_session, psList, 123456789L);  
        }

        [Test]
        public void Bound_Collections_List_Invalids()
        {
            PreparedStatement psList = _session.Prepare(String.Format("INSERT INTO {0} (id, list_sample) VALUES (?, ?)", AllTypesTableName));

            // Invalid cases
            List<Tuple<object, string[]>> objectsAndAssociateExceptionTypes = new List<Tuple<object, string[]>>();
            objectsAndAssociateExceptionTypes.Add(new Tuple<object, string[]>("some string", new string[] {
                "InvalidQueryException", // C* 1.2
                "ServerErrorException", // starting in C* 2.0
                "NoHostAvailableException" // introduced in C* 2.1.2
             }
            )); // object type = string
            if (VersionMatch(new TestCassandraVersion(2, 0), CassandraVersion))
                objectsAndAssociateExceptionTypes.Add(new Tuple<object, string[]>(1, new string[] { 
                    "InvalidQueryException",
                    "NoHostAvailableException" // introduced in C* 2.1.2
                })); // object type = int

            foreach (var objAndExceptionType in objectsAndAssociateExceptionTypes)
                AssertExceptionTypeIsThrown(_session, psList, objAndExceptionType.Item1, objAndExceptionType.Item2);

        }

        [Test]
        public void Bound_Collections_Map_Valid()
        {
            PreparedStatement psMap = _session.Prepare(String.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)", AllTypesTableName));
            AssertValid(_session, psMap, new Dictionary<string, string> { { "one", "1" }, { "two", "2" } });
        }

        /// <summary>
        /// Attempt to bind invalid / compatible value types to a Map column
        /// </summary>
        [Test]
        public void Bound_Collections_Map_Invalids()
        {
            PreparedStatement psMap = _session.Prepare(String.Format("INSERT INTO {0} (id, map_sample) VALUES (?, ?)", AllTypesTableName));
            
            // Invalid cases
            List<Tuple<object, string[]>> objectsAndAssociateExceptionTypes = new List<Tuple<object, string[]>>();
            objectsAndAssociateExceptionTypes.Add(new Tuple<object, string[]>(new List<string>(new[] {"one", "two", "three"}), new[] {"InvalidTypeException"})); // object type = List<String>
            objectsAndAssociateExceptionTypes.Add(
                new Tuple<object, string[]>(
                    "some string", 
                    new[] {
                        "ServerErrorException", // for 2.0
                        "InvalidQueryException" // for 1.2
                    })
                ); // object type = string
            if (VersionMatch(new TestCassandraVersion(2,0), CassandraVersion))
                objectsAndAssociateExceptionTypes.Add(new Tuple<object, string[]>(1, new[] { "InvalidQueryException" })); // object type = int

            foreach (var objAndExceptionType in objectsAndAssociateExceptionTypes)
                AssertExceptionTypeIsThrown(_session, psMap, objAndExceptionType.Item1, objAndExceptionType.Item2);
        }

        [Test]
        public void Bound_ExtraParameter()
        {
            var ps = _session.Prepare(String.Format("INSERT INTO {0} (id, list_sample, int_sample) VALUES (?, ?, ?)", AllTypesTableName));
            Assert.Throws(Is
                .InstanceOf<ArgumentException>().Or
                .InstanceOf<InvalidQueryException>().Or
                .InstanceOf<ServerErrorException>(),
                () => _session.Execute(ps.Bind(Guid.NewGuid(), null, null, "yeah, this is extra")));
        }

        [Test, Timeout(180000), Ignore("This test is triggering the infinite re-prepare loop, not sure if it's supposed to be here")]
        public void Bound_With_ChangingKeyspace()
        {
            using (var localCluster = Cluster.Builder()
                .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(15000))
                .AddContactPoint(_testCluster.InitialContactPoint)
                .Build())
            {
                var session = localCluster.Connect("system");
                session.Execute("CREATE KEYSPACE bound_changeks_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}");
                TestUtils.WaitForSchemaAgreement(localCluster);
                var ps = session.Prepare("SELECT * FROM system.local");
                session.ChangeKeyspace("bound_changeks_test");
                Assert.DoesNotThrow(() => TestHelper.ParallelInvoke(() => session.Execute(ps.Bind()), 10));
            }
        }

        [Test]
        public void Prepared_SelectOne()
        {
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"
                    CREATE TABLE {0}(
                    tweet_id int PRIMARY KEY,
                    numb double,
                    label text);", tableName));
                TestUtils.WaitForSchemaAgreement(_session.Cluster);
            }
            catch (AlreadyExistsException)
            {
            }

            for (int i = 0; i < 10; i++)
            {
                _session.Execute(string.Format("INSERT INTO {0} (tweet_id, numb, label) VALUES({1}, 0.01,'{2}')", tableName, i, "row" + i));
            }

            var prepSelect = QueryTools.PrepareQuery(_session, string.Format("SELECT * FROM {0} WHERE tweet_id = ?;", tableName));

            var rowId = 5;
            var result = QueryTools.ExecutePreparedSelectQuery(_session, prepSelect, new object[1] { rowId });
            foreach (var row in result)
            {
                Assert.True((string)row.GetValue(typeof(int), "label") == "row" + rowId);
            }
            Assert.True(result.Columns != null);
            Assert.True(result.Columns.Length == 3);
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }

        [Test]
        public void Prepared_Massive()
        {
            massivePreparedStatementTest();
        }

        [Test]
        public void Prepared_Decimal()
        {
            InsertingSingleValuePrepared(typeof (Decimal));
        }

        [Test]
        public void Prepared_VarInt()
        {
            InsertingSingleValuePrepared(typeof (BigInteger));
        }

        [Test]
        public void Prepared_BigInt()
        {
            InsertingSingleValuePrepared(typeof (Int64));
        }

        [Test]
        public void Prepared_Double()
        {
            InsertingSingleValuePrepared(typeof (Double));
        }

        [Test]
        public void Prepared_Float()
        {
            InsertingSingleValuePrepared(typeof (Single));
        }

        [Test]
        public void Prepared_Int()
        {
            InsertingSingleValuePrepared(typeof(Int32));
        }

        [Test]
        public void Prepared_Int_Null()
        {
            InsertingSingleValuePrepared(typeof(Int32), null);
        }

        [Test]
        public void Prepared_Varchar_Null()
        {
            InsertingSingleValuePrepared(typeof(string), null);
        }

        [Test]
        public void Prepared_Varchar()
        {
            InsertingSingleValuePrepared(typeof (String));
        }

        [Test]
        public void Prepared_Boolean()
        {
            InsertingSingleValuePrepared(typeof (Boolean));
        }

        [Test]
        public void Prepared_Blob()
        {
            InsertingSingleValuePrepared(typeof (Byte));
        }

        [Test]
        public void Prepared_IpAddress()
        {
            InsertingSingleValuePrepared(typeof(System.Net.IPAddress));
        }

        [Test]
        public void Prepared_UUID()
        {
            InsertingSingleValuePrepared(typeof (Guid));
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

        private static void AssertExceptionTypeIsThrown(ISession session, PreparedStatement preparedStatement, object obj, string[] expectedExceptionClassNames)
        {
            try
            {
                session.Execute(preparedStatement.Bind(Guid.NewGuid(), obj));
            }
            catch (Exception e)
            {
                Assert.IsTrue(expectedExceptionClassNames.Contains(e.GetType().Name), "Current exception type: " + e.GetType() + " was not contained in array: " + string.Join(",", expectedExceptionClassNames));
                return;
            }
            Assert.Fail("Expected exception of one of these types: " + string.Join(",", expectedExceptionClassNames) + " was not thrown for object with type: " + obj.GetType());
        }

        private void AssertValid(ISession session, PreparedStatement ps, object value)
        {
            try
            {
                RowSet rowSet = session.Execute(ps.Bind(Guid.NewGuid(), value));
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
                nonShareableSession.WaitForSchemaAgreement(
                    nonShareableSession.Execute("CREATE TABLE " + fqKeyspaceName + "test(k text PRIMARY KEY, i int)")
                    );
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
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", testCluster.Builder, 60);

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

            QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"CREATE TABLE {0}(
                tweet_id uuid PRIMARY KEY,
                value {1}
                );", tableName, cassandraDataTypeName));

            TestUtils.WaitForSchemaAgreement(_session.Cluster);

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof (string))
                val = "'" + val.ToString().Replace("'", "''") + "'";

            var row1 = new object[2] {Guid.NewGuid(), val};

            toInsert.Add(row1);

            var prep = QueryTools.PrepareQuery(_session,
                                                             string.Format("INSERT INTO {0}(tweet_id, value) VALUES ({1}, ?);", tableName,
                                                                           toInsert[0][0]));
            if (value == null)
            {
                QueryTools.ExecutePreparedQuery(_session, prep, new object[] { toInsert[0][1] });
            }
            else
            {
                QueryTools.ExecutePreparedQuery(_session, prep, new object[] {value});
            }

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert);
            QueryTools.ExecuteSyncNonQuery(_session, string.Format("DROP TABLE {0};", tableName));
        }

        public void massivePreparedStatementTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");

            try
            {
                QueryTools.ExecuteSyncNonQuery(_session, string.Format(@"CREATE TABLE {0}(
                    tweet_id uuid PRIMARY KEY,
                    numb1 double,
                    numb2 int
                    );", tableName));
                TestUtils.WaitForSchemaAgreement(_session.Cluster);
            }
            catch (AlreadyExistsException)
            {
            }
            var numberOfPrepares = 100;

            var values = new List<object[]>(numberOfPrepares);
            var prepares = new List<PreparedStatement>();

            Parallel.For(0, numberOfPrepares, i =>
            {
                PreparedStatement prep = QueryTools.PrepareQuery(_session,
                                                                 string.Format("INSERT INTO {0}(tweet_id, numb1, numb2) VALUES ({1}, ?, ?);",
                                                                               tableName, Guid.NewGuid()));

                lock (prepares)
                    prepares.Add(prep);
            });

            Parallel.ForEach(prepares,
                             prep =>
                             {
                                 QueryTools.ExecutePreparedQuery(_session, prep,
                                                                 new object[]
                                                                 {(double) Randomm.RandomVal(typeof (double)), (int) Randomm.RandomVal(typeof (int))});
                             });

            QueryTools.ExecuteSyncQuery(_session, string.Format("SELECT * FROM {0};", tableName),
                                        _session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}
