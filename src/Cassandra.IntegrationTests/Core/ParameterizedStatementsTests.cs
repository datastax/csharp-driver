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

﻿using System.Linq;
﻿using Cassandra.IntegrationTests.TestBase;
﻿using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    [TestCassandraVersion(2, 0)]
    public class ParameterizedStatementsTests : SharedClusterTest
    {
        private const string AllTypesTableName = "all_types_table_queryparams";

        protected override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            Session.Execute(String.Format(TestUtils.CreateTableAllTypes, AllTypesTableName));
        }

        [Test]
        public void CollectionParamsTests()
        {
            var id = Guid.NewGuid();
            var map = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", "Beatles" } };
            var list = new List<string> { "one", "two" };
            var set = new List<string> { "set_1one", "set_2two" };

            var insertStatement = new SimpleStatement(
                String.Format("INSERT INTO {0} (id, map_sample, list_sample, set_sample) VALUES (?, ?, ?, ?)", AllTypesTableName), id, map, list, set);
            Session.Execute(insertStatement);
            var row = Session.Execute(new SimpleStatement(String.Format("SELECT * FROM {0} WHERE id = ?", AllTypesTableName), id)).First();
            CollectionAssert.AreEquivalent(map, row.GetValue<IDictionary<string, string>>("map_sample"));
            CollectionAssert.AreEquivalent(list, row.GetValue<List<string>>("list_sample"));
            CollectionAssert.AreEquivalent(set, row.GetValue<List<string>>("set_sample"));
        }

        [Test]
        public void CollectionParamsBindTests()
        {
            var id = Guid.NewGuid();
            var map = new SortedDictionary<string, string> { { "fruit", "apple" }, { "band", "Beatles" } };
            var list = new List<string> { "one", "two" };
            var set = new List<string> { "set_1one", "set_2two" };

            var insertStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, map_sample, list_sample, set_sample) VALUES (?, ?, ?, ?)", AllTypesTableName), id, map, list, set);
            Session.Execute(insertStatement);
            var row = Session.Execute(new SimpleStatement(String.Format("SELECT * FROM {0} WHERE id = ?", AllTypesTableName), id)).First();
            CollectionAssert.AreEquivalent(map, row.GetValue<IDictionary<string, string>>("map_sample"));
            CollectionAssert.AreEquivalent(list, row.GetValue<List<string>>("list_sample"));
            CollectionAssert.AreEquivalent(set, row.GetValue<List<string>>("set_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void SimpleStatementSetTimestamp()
        {
            var timestamp = new DateTimeOffset(1999, 12, 31, 1, 2, 3, TimeSpan.Zero);
            var id = Guid.NewGuid();
            var insertStatement = new SimpleStatement(String.Format("INSERT INTO {0} (id, text_sample) VALUES (?, ?)", AllTypesTableName), id, "sample text");
            Session.Execute(insertStatement.SetTimestamp(timestamp));
            var row = Session.Execute(new SimpleStatement(String.Format("SELECT id, text_sample, writetime(text_sample) FROM {0} WHERE id = ?", AllTypesTableName), id)).First();
            Assert.NotNull(row.GetValue<string>("text_sample"));
            Assert.AreEqual(TypeCodec.ToUnixTime(timestamp).Ticks / 10, row.GetValue<object>("writetime(text_sample)"));
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void SimpleStatementNamedValues()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);
            var id = Guid.NewGuid();
            var statement = new SimpleStatement(insertQuery, new { my_int = 100, my_bigint = -500L, my_id = id, my_text = "named params ftw again!" });
            Session.Execute(statement);

            var row = Session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(-500L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("named params ftw again!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void SimpleStatementNamedValuesBind()
        {
            var insertQuery = String.Format("INSERT INTO {0} (text_sample, int_sample, bigint_sample, id) VALUES (:my_text, :my_int, :my_bigint, :my_id)", AllTypesTableName);

            var id = Guid.NewGuid();
            Session.Execute(
                new SimpleStatement(insertQuery,
                    new { my_int = 100, my_bigint = -500L, my_id = id, my_text = "named params ftw again!" }));

            var row = Session.Execute(String.Format("SELECT int_sample, bigint_sample, text_sample FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();

            Assert.AreEqual(100, row.GetValue<int>("int_sample"));
            Assert.AreEqual(-500L, row.GetValue<long>("bigint_sample"));
            Assert.AreEqual("named params ftw again!", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void SimpleStatementNamedValuesCaseInsensitivity()
        {
            var insertQuery = String.Format("INSERT INTO {0} (id, \"text_sample\", int_sample) VALUES (:my_ID, :my_TEXT, :MY_INT)", AllTypesTableName);
            var id = Guid.NewGuid();
            Session.Execute(
                new SimpleStatement(
                    insertQuery,
                    new { my_INt = 1, my_TEXT = "WAT1", my_id = id}));

            var row = Session.Execute(String.Format("SELECT * FROM {0} WHERE id = {1:D}", AllTypesTableName, id)).First();
            Assert.AreEqual(1, row.GetValue<int>("int_sample"));
            Assert.AreEqual("WAT1", row.GetValue<string>("text_sample"));
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void SimpleStatementNamedValuesNotSpecified()
        {
            var insertQuery = String.Format("INSERT INTO {0} (float_sample, text_sample, bigint_sample, id) VALUES (:MY_float, :my_TexT, :my_BIGint, :id)", AllTypesTableName);

            Assert.Throws<InvalidQueryException>(() => Session.Execute(
                new SimpleStatement(insertQuery, 
                    new {id = Guid.NewGuid(), my_bigint = 1L })));
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void SimpleStatementSmallIntTests()
        {
            Session.Execute("CREATE TABLE tbl_smallint_param (id int PRIMARY KEY, v smallint, m map<smallint, text>)");
            var values = new short[] { Int16.MinValue, -3, -2, 0, 1, 2, 0xff, 0x0101, Int16.MaxValue };
            foreach (var v in values)
            {
                var insert = new SimpleStatement("INSERT INTO tbl_smallint_param (id, v, m) VALUES (?, ?, ?)",
                    Convert.ToInt32(v), v, new SortedDictionary<short, string> { { v, v.ToString() } });
                var select = new SimpleStatement("SELECT * FROM tbl_smallint_param WHERE id = ?", Convert.ToInt32(v));
                Session.Execute(insert);
                var rs = Session.Execute(select).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<short>("v"));
                Assert.AreEqual(v.ToString(), rs[0].GetValue<SortedDictionary<short, string>>("m")[v]);
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void SimpleStatementTinyIntTests()
        {
            Session.Execute("CREATE TABLE tbl_tinyint_param (id int PRIMARY KEY, v tinyint, m map<tinyint, text>)");
            var values = new sbyte[] { sbyte.MinValue, -4, -3, 0, 1, 2, 126, sbyte.MaxValue };
            foreach (var v in values)
            {
                var insert = new SimpleStatement("INSERT INTO tbl_tinyint_param (id, v, m) VALUES (?, ?, ?)", 
                    Convert.ToInt32(v), v, new SortedDictionary<sbyte, string> { { v, v.ToString()} });
                var select = new SimpleStatement("SELECT * FROM tbl_tinyint_param WHERE id = ?", Convert.ToInt32(v));
                Session.Execute(insert);
                var rs = Session.Execute(select).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<sbyte>("v"));
                Assert.AreEqual(v.ToString(), rs[0].GetValue<SortedDictionary<sbyte, string>>("m")[v]);
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void SimpleStatementDateTests()
        {
            Session.Execute("CREATE TABLE tbl_date_param (id int PRIMARY KEY, v date, m map<date, text>)");
            var values = new[] { 
                new LocalDate(2010, 4, 29),
                new LocalDate(0, 3, 12),
                new LocalDate(-10, 2, 4),
                new LocalDate(5881580, 7, 11),
                new LocalDate(-5877641, 6, 23) 
            };
            for (var i = 0; i < values.Length; i++)
            {
                var v = values[i];
                var insert = new SimpleStatement("INSERT INTO tbl_date_param (id, v, m) VALUES (?, ?, ?)",
                    i, v, new SortedDictionary<LocalDate, string> { { v, v.ToString() } });
                var select = new SimpleStatement("SELECT * FROM tbl_date_param WHERE id = ?", i);
                Session.Execute(insert);
                var rs = Session.Execute(select).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<LocalDate>("v"));
                Assert.AreEqual(v.ToString(), rs[0].GetValue<SortedDictionary<LocalDate, string>>("m")[v]);
            }
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void SimpleStatementTimeTests()
        {
            Session.Execute("CREATE TABLE tbl_time_param (id int PRIMARY KEY, v time, m map<time, text>)");
            var values = new[] {
                new LocalTime(0, 0, 0, 0),
                new LocalTime(0, 1, 1, 789),
                new LocalTime(6, 1, 59, 0),
                new LocalTime(10, 31, 5, 789776),
                new LocalTime(23, 59, 59, 999999999),
            };
            for (var i = 0; i < values.Length; i++)
            {
                var v = values[i];
                var insert = new SimpleStatement("INSERT INTO tbl_time_param (id, v, m) VALUES (?, ?, ?)",
                    i, v, new SortedDictionary<LocalTime, string> { { v, v.ToString() } });
                var select = new SimpleStatement("SELECT * FROM tbl_time_param WHERE id = ?", i);
                Session.Execute(insert);
                var rs = Session.Execute(select).ToList();
                Assert.AreEqual(1, rs.Count);
                Assert.AreEqual(v, rs[0].GetValue<LocalTime>("v"));
                Assert.AreEqual(v.ToString(), rs[0].GetValue<SortedDictionary<LocalTime, string>>("m")[v]);
            }
        }

        [Test]
        public void Text()
        {
            ParameterizedStatement(typeof(string));
        }

        [Test]
        public void Blob()
        {
            ParameterizedStatement(typeof(byte));
        }

        [Test]
        public void ASCII()
        {
            ParameterizedStatement(typeof(Char));
        }

        [Test]
        public void Decimal()
        {
            ParameterizedStatement(typeof(Decimal));
        }

        [Test]
        public void VarInt()
        {
            ParameterizedStatement(typeof(BigInteger));
        }

        [Test]
        public void BigInt()
        {
            ParameterizedStatement(typeof(Int64));
        }

        [Test]
        public void Double()
        {
            ParameterizedStatement(typeof(Double));
        }

        [Test]
        public void Float()
        {
            ParameterizedStatement(typeof(Single));
        }

        [Test]
        public void Int()
        {
            ParameterizedStatement(typeof(Int32));
        }

        [Test]
        public void Boolean()
        {
            ParameterizedStatement(typeof(Boolean));
        }

        [Test]
        public void UUID()
        {
            ParameterizedStatement(typeof(Guid));
        }

        [Test]
        public void TimeStamp()
        {
            ParameterizedStatementTimeStamp();
        }

        [Test]
        public void IntAsync()
        {
            ParameterizedStatement(typeof(Int32), true);
        }

        private void ParameterizedStatementTimeStamp()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var expectedValues = new List<object[]>(1);
            var valuesToTest = new List<object[]> { new object[] { Guid.NewGuid(), new DateTimeOffset(2011, 2, 3, 16, 5, 0, new TimeSpan(0000)) },
                                                    {new object[] {Guid.NewGuid(), (long)0}}};

            int x = 0;
            foreach (var bindValues in valuesToTest)
            {
                expectedValues.Add(bindValues);
                CreateTable(tableName, "timestamp");
                var statement = new SimpleStatement(String.Format("INSERT INTO {0} (id, val) VALUES (?, ?)", tableName), bindValues);
                Session.Execute(statement);

                // Verify results
                RowSet rs = Session.Execute("SELECT * FROM " + tableName);
                VerifyData(rs, expectedValues);
                
                Session.Execute(String.Format("DROP TABLE {0}", tableName));
                expectedValues.Clear();
            }
        }

        private void ParameterizedStatement(Type type, bool testAsync = false)
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(type);
            var expectedValues = new List<object[]>(1);
            var val = Randomm.RandomVal(type);
            var bindValues = new object[] { Guid.NewGuid(), val };
            expectedValues.Add(bindValues);
            
            CreateTable(tableName, cassandraDataTypeName);

            var statement = new SimpleStatement(String.Format("INSERT INTO {0} (id, val) VALUES (?, ?)", tableName), bindValues);

            if (testAsync)
            {
                Session.ExecuteAsync(statement).Wait(500);
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
                QueryTools.ExecuteSyncNonQuery(Session, string.Format(@"CREATE TABLE {0}(
                                                                        id uuid PRIMARY KEY,
                                                                        val {1}
                                                                        );", tableName, type));
            }
            catch (AlreadyExistsException)
            {
            }
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
        #pragma warning restore 618
    }
}
