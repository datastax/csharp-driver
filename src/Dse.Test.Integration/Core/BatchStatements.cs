//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Serialization;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    public class BatchStatements : SimulacronTest
    {
        private readonly string _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();

        /// <summary>
        /// Use a 3-node cluster to test prepared batches (unprepared flow).
        /// </summary>
        public BatchStatements() : base(options: new SimulacronOptions { Nodes = "3" })
        {
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement(bool async)
        {
            var expectedValues = new List<object[]> { new object[] { 1, "label1", 1 }, new object[] { 2, "label2", 2 }, new object[] { 3, "label3", 3 } };

            var ps =
                async
                    ? Session.PrepareAsync($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)").GetAwaiter().GetResult()
                    : Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            foreach (var val in expectedValues)
            {
                batch.Add(ps.Bind(val));
            }

            if (async)
            {
                Session.ExecuteAsync(batch).GetAwaiter().GetResult();
            }
            else
            {
                Session.Execute(batch);
            }

            VerifyBatchStatement(
                1,
                expectedValues.Select(v => ps.Id).ToArray(),
                expectedValues.ToArray());

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT * FROM {_tableName} WHERE id IN ({1}, {2}, {3})")
                      .ThenRowsSuccess(new[] { "id", "label", "number" }, r => r.WithRows(expectedValues.ToArray())));

            // Verify results
            var rs = async
                ? Session.ExecuteAsync(new SimpleStatement($"SELECT * FROM {_tableName} WHERE id IN ({1}, {2}, {3})")).GetAwaiter().GetResult()
                : Session.Execute($"SELECT * FROM {_tableName} WHERE id IN ({1}, {2}, {3})");

            VerifyData(rs, expectedValues);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatementSingle()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N").ToLower();

            var batch = new BatchStatement();
            var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({20}, '{"label 20"}', {20})");
            batch.Add(simpleStatement);
            Session.Execute(batch);

            VerifyBatchStatement(
                1,
                new[] { $"INSERT INTO {_tableName} (id, label, number) VALUES ({20}, '{"label 20"}', {20})" });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Bound()
        {
            var batch = new BatchStatement();

            var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
#pragma warning disable 618
            batch.Add(simpleStatement.Bind(100, "label 100", 10000));
#pragma warning restore 618
            Session.Execute(batch);

            VerifyBatchStatement(
                1,
                new[] { $"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)" },
                new object[] { 100, "label 100", 10000 });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_SimpleStatement_Multiple()
        {
            var batch = new BatchStatement();
            var range = Enumerable.Range(200, 5);

            foreach (var x in range)
            {
                var simpleStatement = new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({x}, '{"label" + x}', {x * x})");
                batch.Add(simpleStatement);
            }
            Session.Execute(batch);

            VerifyBatchStatement(
                1,
                range.Select(x => $"INSERT INTO {_tableName} (id, label, number) VALUES ({x}, '{"label" + x}', {x * x})").ToArray());
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables()
        {
            var batch = new BatchStatement();

            batch.Add(new SimpleStatement($@"INSERT INTO table1 (id, label, number) VALUES ({1}, '{"label1"}', {1})"));
            batch.Add(new SimpleStatement($@"INSERT INTO table2 (id, label, number) VALUES ({2}, '{"label2"}', {2})"));

            Session.Execute(batch);

            VerifyBatchStatement(
                1,
                new[]
                {
                    $@"INSERT INTO table1 (id, label, number) VALUES ({1}, '{"label1"}', {1})",
                    $@"INSERT INTO table2 (id, label, number) VALUES ({2}, '{"label2"}', {2})"
                });
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_UsingTwoTables_OneInvalidTable()
        {
            var batch = new BatchStatement();

            TestCluster.PrimeFluent(
                b => b.WhenBatch(when => when.WithQueries(
                          $@"INSERT INTO {_tableName} (id, label, number) VALUES ({400}, '{"label1"}', {1})",
                          $@"INSERT INTO table_randomnonexistent (id, label, number) VALUES ({2}, '{"label2"}', {2})"))
                      .ThenServerError(ServerError.Invalid, "msg"));

            batch.Add(new SimpleStatement($@"INSERT INTO {_tableName} (id, label, number) VALUES ({400}, '{"label1"}', {1})"));
            batch.Add(new SimpleStatement($@"INSERT INTO table_randomnonexistent (id, label, number) VALUES ({2}, '{"label2"}', {2})"));

            Assert.Throws<InvalidQueryException>(() => Session.Execute(batch));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_MixedStatement()
        {
            var simpleStatement =
                new SimpleStatement($"INSERT INTO {_tableName} (id, label, number) VALUES ({500}, {"label 500"}, {2})");
            var ps = Session.Prepare($@"INSERT INTO {_tableName} (id, label, number) VALUES (?, ?, ?)");
            var batchStatement = new BatchStatement();

            batchStatement.Add(ps.Bind(501, "test", 2));
            batchStatement.Add(simpleStatement);

            Session.Execute(batchStatement);

            VerifyBatchStatement(
                1,
                new[]
                {
                    Convert.ToBase64String(ps.Id),
                    $"INSERT INTO {_tableName} (id, label, number) VALUES ({500}, {"label 500"}, {2})"
                },
                new[]
                {
                    new object [] { 501, "test", 2 },
                    new object [] { }
                });
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_SerialConsistency()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");

            Assert.Throws<ArgumentException>(() =>
            {
                //You can not specify local serial consistency as a valid read one.
                var batch = new BatchStatement()
                    .Add(query)
                    .SetBatchType(BatchType.Logged)
                    .SetSerialConsistencyLevel(ConsistencyLevel.Quorum);
                Session.Execute(batch);
            });

            //It should work
            var statement = new BatchStatement()
                .Add(query)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);

            //Read consistency specified and write consistency specified
            Session.Execute(statement);

            VerifyBatchStatement(
                1,
                new[] { $"INSERT INTO {_tableName} (id) values (-99999)" },
                msg => msg.ConsistencyLevel == ConsistencyLevel.Quorum && msg.SerialConsistencyLevel == ConsistencyLevel.LocalSerial);
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_Timestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");
            var dt = DateTime.Now;
            Assert.DoesNotThrow(() =>
            {
                //It should work
                var statement = new BatchStatement()
                    .Add(query)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum)
                    .SetTimestamp(dt);

                //Read consistency specified and write consistency specified
                Session.Execute(statement);
            });

            VerifyBatchStatement(
                1,
                new[] { $"INSERT INTO {_tableName} (id) values (-99999)" },
                msg => msg.ConsistencyLevel == ConsistencyLevel.Quorum
                       && msg.DefaultTimestamp == SimulacronAPI.DataType.GetMicroSecondsTimestamp(dt));
        }
        
        [Test]
        [TestCassandraVersion(2, 0)]
        public void Batch_PreparedStatement_Large()
        {
            var tableName = "table" + Guid.NewGuid().ToString("N");

            var ps = Session.Prepare($@"INSERT INTO {tableName} (id, label, number) VALUES (?, ?, ?)");
            var batch = new BatchStatement();
            var expectedValues = new List<object[]>();

            var range = Enumerable.Range(1, 100);

            foreach (var x in range)
            {
                expectedValues.Add(new object[] { x, "value" + x, x });
                batch.Add(ps.Bind(new object[] { x, "value" + x, x }));
            }

            Session.Execute(batch);

            VerifyBatchStatement(
                1,
                range.Select(x => Convert.ToBase64String(ps.Id)).ToArray(),
                expectedValues.ToArray());
        }

        private static void VerifyData(IEnumerable<Row> rowSet, List<object[]> expectedValues)
        {
            var x = 0;
            foreach (var row in rowSet)
            {
                var y = 0;
                var objArr = expectedValues[x];

                using (var rowEnum = row.GetEnumerator())
                {
                    while (rowEnum.MoveNext())
                    {
                        var current = rowEnum.Current;
                        Assert.AreEqual(objArr[y], current, $"Found difference between expected and actual row {objArr[y]} != {current}");
                        y++;
                    }
                }
                x++;
            }
        }
    }

    //Mock generator
    internal class MockTimestampGenerator : ITimestampGenerator
    {
        private DateTime _initialDateTime = DateTime.Parse("2017-01-01T00:00:01-3:00");
        private const long TicksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;

        public long Next()
        {
            return (_initialDateTime.Ticks - TypeSerializer.UnixStart.UtcTicks) / TicksPerMicrosecond;
        }
    }
}