//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using Dse.Test.Integration.TestClusterManagement;
using System.Reflection;
using Dse.Test.Integration.SimulacronAPI.Models.Logs;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    /// <summary>
    /// Validates the (de)serialization of CRL types and CQL types.
    /// Each test will upsert a value on specific CQL type and expect the correspondent CRL type. Should_Get(CRL type)_When_Upsert(CQL data type).
    /// </summary>
    public class BasicTypeTests : SimulacronTest
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
            const string insertQuery = @"INSERT INTO decimal_neg_scale (id, value) VALUES (?, ?)";
            var preparedStatement = Session.Prepare(insertQuery);

            const int scale = -1;
            var scaleBytes = BitConverter.GetBytes(scale);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(scaleBytes);
            }

            var bytes = new byte[scaleBytes.Length + 1];
            Array.Copy(scaleBytes, bytes, scaleBytes.Length);

            bytes[scaleBytes.Length] = 5;

            var firstRowValues = new object[] { Guid.NewGuid(), bytes };
            Session.Execute(preparedStatement.Bind(firstRowValues));

            VerifyBoundStatement(
                insertQuery,
                1,
                firstRowValues);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM decimal_neg_scale")
                      .ThenRowsSuccess(new[] {"id", "value"}, r => r.WithRow(firstRowValues.First(), (decimal) 50)));

            var row = Session.Execute("SELECT * FROM decimal_neg_scale").First();
            var decValue = row.GetValue<decimal>("value");
            
            Assert.AreEqual(50, decValue);
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

                toInsertAndCheck[0][2] = minimum;
                toInsertAndCheck[1][2] = maximum;

                if (!sameOutput) //for ExceedingCassandra_FLOAT() test case
                {
                    toInsertAndCheck[0][2] = Single.NegativeInfinity;
                    toInsertAndCheck[1][2] = Single.PositiveInfinity;
                }
            }

            try
            {
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[0][0]}, '{toInsertAndCheck[0][1]}', {toInsertAndCheck[0][2]});", null);
                QueryTools.ExecuteSyncNonQuery(Session,
                    $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[1][0]}, '{toInsertAndCheck[1][1]}', {toInsertAndCheck[1][2]});", null);
            }
            catch (InvalidQueryException)
            {
                if (!sameOutput && toExceed == typeof(Int32)) //for ExceedingCassandra_INT() test case
                {
                    return;
                }

                throw;
            }

            VerifyStatement(
                QueryType.Query,
                $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[0][0]}, '{toInsertAndCheck[0][1]}', {toInsertAndCheck[0][2]});",
                1);

            VerifyStatement(
                QueryType.Query,
                $"INSERT INTO {tableName}(tweet_id, label, number) VALUES ({toInsertAndCheck[1][0]}, '{toInsertAndCheck[1][1]}', {toInsertAndCheck[1][2]});",
                1);

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM {tableName};", when => when.WithConsistency(ConsistencyLevel.One))
                      .ThenRowsSuccess(
                          new[] {"tweet_id", "label", "number"}, 
                          r => r.WithRow(toInsertAndCheck[0][0], toInsertAndCheck[0][1], toInsertAndCheck[0][2])
                                .WithRow(toInsertAndCheck[1][0], toInsertAndCheck[1][1], toInsertAndCheck[1][2])));

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};", ConsistencyLevel.One, toInsertAndCheck);
        }


        public void TestCounters()
        {
            var tableName = TestUtils.GetUniqueTableName();

            var tweet_id = Guid.NewGuid();

            var tasks = Enumerable.Range(0, 100).Select(i => Task.Factory.StartNew(
                () =>
                {
                    QueryTools.ExecuteSyncNonQuery(Session,
                        string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                            tweet_id, (i % 2 == 0 ? "-" : "+") + i));
                },
                TaskCreationOptions.LongRunning | TaskCreationOptions.HideScheduler));

            Task.WaitAll(tasks.ToArray());

            var logs = TestCluster.GetQueries(null, QueryType.Query).Where(l => l.Query.StartsWith("UPDATE")).ToList();

            Assert.AreEqual(100, logs.Count);
            foreach (var i in Enumerable.Range(0, 100))
            {
                var query = string.Format(@"UPDATE {0} SET incdec = incdec {2}  WHERE tweet_id = {1};", tableName,
                    tweet_id, (i % 2 == 0 ? "-" : "+") + i);
                VerifyStatement(logs, query, 1);
            }

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM {tableName};", when => when.WithConsistency(ConsistencyLevel.LocalOne))
                      .ThenRowsSuccess(new[] {"tweet_id", "incdec"}, r => r.WithRow(tweet_id, (Int64) 50)));

            QueryTools.ExecuteSyncQuery(Session, $"SELECT * FROM {tableName};",
                                        Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(),
                                        new List<object[]> { new object[2] { tweet_id, (Int64)50 } });
        }

        public void InsertingSingleValue(Type tp)
        {
            var tableName = TestUtils.GetUniqueTableName();

            var toInsert = new List<object[]>(1);
            var val = Randomm.RandomVal(tp);
            if (tp == typeof(string))
            {
                val = "'" + val.ToString().Replace("'", "''") + "'";
            }

            var row1 = new object[2] { Guid.NewGuid(), val };
            toInsert.Add(row1);

            var isFloatingPoint = false;

            if (row1[1].GetType() == typeof(string) || row1[1].GetType() == typeof(byte[]))
            {
                var query =
                    $"INSERT INTO {tableName}(tweet_id,value) VALUES (" +
                        $"{toInsert[0][0]}, " +
                        $"{(row1[1].GetType() == typeof(byte[]) ? "0x" + CqlQueryTools.ToHex((byte[]) toInsert[0][1]) : "'" + toInsert[0][1] + "'")}" +
                        ");";
                QueryTools.ExecuteSyncNonQuery(Session, query, null);
                VerifyStatement(QueryType.Query, query, 1);
            }
            else
            {
                if (tp == typeof(Single) || tp == typeof(Double))
                {
                    isFloatingPoint = true;
                }

                var query =
                    $"INSERT INTO {tableName}(tweet_id,value) VALUES (" +
                        $"{toInsert[0][0]}, " +
                        $"{(!isFloatingPoint ? toInsert[0][1] : toInsert[0][1].GetType().GetMethod("ToString", new[] { typeof(string) }).Invoke(toInsert[0][1], new object[] { "r" }))}" +
                        ");";
                QueryTools.ExecuteSyncNonQuery(Session, query, null);
                VerifyStatement(QueryType.Query, query, 1);
            }

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM {tableName};", when => when.WithConsistency(ConsistencyLevel.LocalOne))
                      .ThenRowsSuccess(new[] {"tweet_id", "value"}, r => r.WithRow(toInsert[0][0], toInsert[0][1])));

            QueryTools.ExecuteSyncQuery(
                Session, $"SELECT * FROM {tableName};", Session.Cluster.Configuration.QueryOptions.GetConsistencyLevel(), toInsert);
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
