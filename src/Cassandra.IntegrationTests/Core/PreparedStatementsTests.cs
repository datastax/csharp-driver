//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Threading.Tasks;
using NUnit.Framework;
using System.Net;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class PreparedStatementsTests : TwoNodesClusterTest
    {
        private const string AllTypesTableName = "all_types_table_prepared";

        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();

            //Create a table that can be reused within this test class
            Session.WaitForSchemaAgreement(Session.Execute(String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, AllTypesTableName)));
        }

        [Test]
        public void BoundStatementsAllTypesDifferentValues()
        {
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);
            
            //TODO: Include inet datatype
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
        public void BoundStatementsAllTypesNullValues()
        {
            var insertQuery = String.Format(@"
                INSERT INTO {0} 
                (id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, 
                    blob_sample, boolean_sample, timestamp_sample, inet_sample) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", AllTypesTableName);

            var preparedStatement = Session.Prepare(insertQuery);

            var nullRowValues = new object[] 
            { 
                Guid.NewGuid(), null, null, null, null, null, null, null, null, null, null
            };

            Session.Execute(preparedStatement.Bind(nullRowValues));

            var rs = Session.Execute(String.Format("SELECT * FROM {0} WHERE id = {1}", AllTypesTableName, nullRowValues[0]));
            var row = rs.First();
            Assert.IsNotNull(row);
            Assert.AreEqual(1, row.Where(v => v != null).Count());
            Assert.IsTrue(row.Where(v => v == null).Count() > 5, "The rest of the row values must be null");
        }

        [Test]
        public void PreparedSelectOneTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");

            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"
                        CREATE TABLE {0}(
                        tweet_id int PRIMARY KEY,
                        numb double,
                        label text);", tableName))
                    );
            }
            catch (AlreadyExistsException)
            {
            }

            for (int i = 0; i < 10; i++)
            {
                Session.Execute(string.Format("INSERT INTO {0} (tweet_id, numb, label) VALUES({1}, 0.01,'{2}')", tableName, i, "row" + i));
            }

            PreparedStatement prep_select = QueryTools.PrepareQuery(Session, string.Format("SELECT * FROM {0} WHERE tweet_id = ?;", tableName));

            int rowID = 5;
            var result = QueryTools.ExecutePreparedSelectQuery(Session, prep_select, new object[1] { rowID });
            foreach (var row in result)
            {
                Assert.True((string)row.GetValue(typeof(int), "label") == "row" + rowID);
            }
            Assert.True(result.Columns != null);
            Assert.True(result.Columns.Length == 3);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        [Test]
        public void TestMassivePrepared()
        {
            massivePreparedStatementTest();
        }

        [Test]
        public void TestPreparedDecimal()
        {
            InsertingSingleValuePrepared(typeof (Decimal));
        }

        [Test]
        public void TestPreparedVarInt()
        {
            InsertingSingleValuePrepared(typeof (BigInteger));
        }

        [Test]
        public void TestPreparedBigInt()
        {
            InsertingSingleValuePrepared(typeof (Int64));
        }

        [Test]
        public void TestPreparedDouble()
        {
            InsertingSingleValuePrepared(typeof (Double));
        }

        [Test]
        public void TestPreparedFloat()
        {
            InsertingSingleValuePrepared(typeof (Single));
        }

        [Test]
        public void TestPreparedInt()
        {
            InsertingSingleValuePrepared(typeof(Int32));
        }

        [Test]
        public void TestPreparedNullInt()
        {
            InsertingSingleValuePrepared(typeof(Int32), null);
        }

        [Test]
        public void TestPreparedNullVarchar()
        {
            InsertingSingleValuePrepared(typeof(string), null);
        }

        [Test]
        public void TestPreparedVarchar()
        {
            InsertingSingleValuePrepared(typeof (String));
        }

        [Test]
        public void TestPreparedBoolean()
        {
            InsertingSingleValuePrepared(typeof (Boolean));
        }

        [Test]
        public void TestPreparedBlob()
        {
            InsertingSingleValuePrepared(typeof (Byte));
        }

        [Test]
        public void TestPreparedInet()
        {
            InsertingSingleValuePrepared(typeof(System.Net.IPAddress));
        }

        [Test]
        public void TestPreparedUUID()
        {
            InsertingSingleValuePrepared(typeof (Guid));
        }

        public void InsertingSingleValuePrepared(Type tp, object value = null)
        {
            string cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            string tableName = "table" + Guid.NewGuid().ToString("N");

            Session.WaitForSchemaAgreement(
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         value {1}
         );", tableName, cassandraDataTypeName))
                );

            var toInsert = new List<object[]>(1);
            object val = Randomm.RandomVal(tp);
            if (tp == typeof (string))
                val = "'" + val.ToString().Replace("'", "''") + "'";

            var row1 = new object[2] {Guid.NewGuid(), val};

            toInsert.Add(row1);

            PreparedStatement prep = QueryTools.PrepareQuery(Session,
                                                             string.Format("INSERT INTO {0}(tweet_id, value) VALUES ({1}, ?);", tableName,
                                                                           toInsert[0][0]));
            if (value == null)
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new object[] { toInsert[0][1] });
            }
            else
            {
                QueryTools.ExecutePreparedQuery(Session, prep, new object[] {value});
            }

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName), ConsistencyLevel.One, toInsert);
            QueryTools.ExecuteSyncNonQuery(Session, string.Format("DROP TABLE {0};", tableName));
        }

        public void massivePreparedStatementTest()
        {
            string tableName = "table" + Guid.NewGuid().ToString("N");

            try
            {
                Session.WaitForSchemaAgreement(
                    QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
         tweet_id uuid PRIMARY KEY,
         numb1 double,
         numb2 int
         );", tableName))
                    );
            }
            catch (AlreadyExistsException)
            {
            }
            int numberOfPrepares = 100;

            var values = new List<object[]>(numberOfPrepares);
            var prepares = new List<PreparedStatement>();

            Parallel.For(0, numberOfPrepares, i =>
            {
                PreparedStatement prep = QueryTools.PrepareQuery(Session,
                                                                 string.Format("INSERT INTO {0}(tweet_id, numb1, numb2) VALUES ({1}, ?, ?);",
                                                                               tableName, Guid.NewGuid()));

                lock (prepares)
                    prepares.Add(prep);
            });

            Parallel.ForEach(prepares,
                             prep =>
                             {
                                 QueryTools.ExecutePreparedQuery(Session, prep,
                                                                 new object[]
                                                                 {(double) Randomm.RandomVal(typeof (double)), (int) Randomm.RandomVal(typeof (int))});
                             });

            QueryTools.ExecuteSyncQuery(Session, string.Format("SELECT * FROM {0};", tableName),
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}