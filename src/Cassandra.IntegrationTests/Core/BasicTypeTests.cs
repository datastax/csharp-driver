//
//      Copyright (C) DataStax Inc.
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
using Cassandra.IntegrationTests.TestBase;
using System.Reflection;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Validates the (de)serialization of CRL types and CQL types.
    /// Each test will upsert a value on specific CQL type and expect the correspondent CRL type. Should_Get(CRL type)_When_Upsert(CQL data type).
    /// </summary>
    [Category("short")]
    public class BasicTypeTests : SharedClusterTest
    {
        [Test]
        public void Should_GetTypeCounter_When_UpsertTypeCounter()
        {
            TestCounters();
        }

        [Test]
        public void Should_GetByte_When_UpsertTypeBlob()
        {
            InsertingSingleValue(typeof(byte));
        }

        [Test]
        public void Should_GetString_When_UpsertTypeASCIIC()
        {
            InsertingSingleValue(typeof(Char));
        }

        [Test]
        public void Should_getTypeDecimal_When_UpsertDecimal()
        {
            InsertingSingleValue(typeof(Decimal));
        }

        [Test]
        public void Should_GetBigInteger_When_UpsertVarInt()
        {
            InsertingSingleValue(typeof(BigInteger));
        }

        [Test]
        public void Should_GetInt64_When_UpsertBigInt()
        {
            InsertingSingleValue(typeof(Int64));
        }

        [Test]
        public void Should_GetDouble_When_UpsertDouble()
        {
            InsertingSingleValue(typeof(Double));
        }

        [Test]
        public void Should_GetFloat_When_UpsertFloat()
        {
            InsertingSingleValue(typeof(Single));
        }

        [Test]
        public void Should_GetInt_When_UpsertInt()
        {
            InsertingSingleValue(typeof(Int32));
        }

        [Test]
        public void Should_GeteBool_When_UpsertBoolean()
        {
            InsertingSingleValue(typeof(Boolean));
        }

        [Test]
        public void Should_GetGuid_When_UpsertUUID()
        {
            InsertingSingleValue(typeof(Guid));
        }

        [Test]
        public void Should_GetDateTimeOffset_When_UpsertTimestamp()
        {
            TimestampTest();
        }

        [Test]
        public void Should_UpsertAndSelectTypeInt_When_InputHighAndLowLimits()
        {
            ExceedingCassandraType(typeof(Int32), typeof(Int32));
        }

        [Test]
        public void Should_UpsertAndSelectTypeBigInt_When_InputHighAndLowLimits()
        {
            ExceedingCassandraType(typeof(Int64), typeof(Int64));
        }

        [Test]
        public void Should_UpsertAndSelectTypeFloat_When_InputHighAndLowLimits()
        {
            ExceedingCassandraType(typeof(Single), typeof(Single));
        }

        [Test]
        public void Should_UpsertAndSelectTypeDouble_When_InputHighAndLowLimits()
        {
            ExceedingCassandraType(typeof(Double), typeof(Double));
        }

        [Test]
        public void Should_ThrowException_When_ExceedingCQLTypeInt()
        {
            ExceedingCassandraType(typeof(Int32), typeof(Int64), false);
        }

        [Test]
        public void Should_ThrowException_When_ExceedingCQLTypeFloat()
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

        public void ExceedingCassandraType(Type toExceed, Type toExceedWith, bool sameOutput = true)
        {
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(toExceed);
            var tableName = TestUtils.GetUniqueTableName();
            var query = $"CREATE TABLE {tableName}(tweet_id uuid PRIMARY KEY, label text, number {cassandraDataTypeName});";
            QueryTools.ExecuteSyncNonQuery(Session, query);

            var minimum = toExceedWith.GetTypeInfo().GetField("MinValue").GetValue(this);
            var maximum = toExceedWith.GetTypeInfo().GetField("MaxValue").GetValue(this);

            var row1 = new object[3] { Guid.NewGuid(), "Minimum", minimum };
            var row2 = new object[3] { Guid.NewGuid(), "Maximum", maximum };
            var toInsertAndCheck = new List<object[]>(2) { row1, row2 };

            if (toExceedWith == typeof(double) || toExceedWith == typeof(Single))
            {
                minimum = minimum.GetType().GetTypeInfo().GetMethod("ToString", new[] { typeof(string) }).Invoke(minimum, new object[1] { "r" });
                maximum = maximum.GetType().GetTypeInfo().GetMethod("ToString", new[] { typeof(string) }).Invoke(maximum, new object[1] { "r" });

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsertAndCheck[0][2] = Single.NegativeInfinity;
                    toInsertAndCheck[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[0][0]}, '{toInsertAndCheck[0][1]}', {minimum});", null);
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[1][0]}, '{toInsertAndCheck[1][1]}', {maximum});", null);
            }
            catch (InvalidQueryException)
            {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    return;
                }
            }

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};", ConsistencyLevel.One, toInsertAndCheck);
        }


        public void TestCounters()
        {
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = $"CREATE TABLE {tableName}(tweet_id uuid PRIMARY KEY, incdec counter);";
                QueryTools.ExecuteSyncNonQuery(Session, query);
            }
            catch (AlreadyExistsException)
            {
            }

            var tweet_id = Guid.NewGuid();

            Parallel.For(0, 100,
                         i =>
                         {
                             QueryTools.ExecuteSyncNonQuery(Session,
                                                            string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                                                                          tweet_id, (i % 2 == 0 ? "-" : "+") + i));
                         });

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};",
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
                                        new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
        }

        public void InsertingSingleValue(Type tp)
        {
            var cassandraDataTypeName = QueryTools.convertTypeNameToCassandraEquivalent(tp);
            var tableName = TestUtils.GetUniqueTableName();
            try
            {
                var query = $@"CREATE TABLE {tableName}(tweet_id uuid PRIMARY KEY, value {cassandraDataTypeName});";
                QueryTools.ExecuteSyncNonQuery(Session, query);
            }
            catch (AlreadyExistsException)
            {
            }

            var toInsert = new List<object[]>(1);
            var val = Randomm.RandomVal(tp);
            if (tp == typeof(string))
                val = "'" + val.ToString().Replace("'", "''") + "'";
            var row1 = new object[2] { Guid.NewGuid(), val };
            toInsert.Add(row1);

            var isFloatingPoint = false;

            if (row1[1].GetType() == typeof(string) || row1[1].GetType() == typeof(byte[]))
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id,value) VALUES ({toInsert[0][0]}, {(row1[1].GetType() == typeof(byte[]) ? "0x" + CqlQueryTools.ToHex((byte[]) toInsert[0][1]) : "'" + toInsert[0][1] + "'")});", null);
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double))
                    isFloatingPoint = true;
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id,value) VALUES ({toInsert[0][0]}, {(!isFloatingPoint ? toInsert[0][1] : toInsert[0][1].GetType().GetMethod("ToString", new[] {typeof(string)}).Invoke(toInsert[0][1], new object[] {"r"}))});", null);
            }

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};",
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), toInsert);
        }

        public void TimestampTest()
        {
            var tableName = TestUtils.GetUniqueTableName();
            var createQuery = $@"CREATE TABLE {tableName}(tweet_id uuid PRIMARY KEY, ts timestamp);";
            QueryTools.ExecuteSyncNonQuery(Session, createQuery);

            QueryTools.ExecuteSyncNonQuery(Session,
                $"INSERT INTO {tableName}(tweet_id,ts) VALUES ({Guid.NewGuid()}, '2011-02-03 04:05+0000');", null);
            QueryTools.ExecuteSyncNonQuery(Session,
                $"INSERT INTO {tableName}(tweet_id,ts) VALUES ({Guid.NewGuid()}, '{220898707200000}');", null);
            QueryTools.ExecuteSyncNonQuery(Session, $"INSERT INTO {tableName}(tweet_id,ts) VALUES ({Guid.NewGuid()}, '{0}');",
                                           null);

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};",
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel());
        }
    }
}
