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

using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [TestCassandraVersion(3, 10)]
    public class GroupByTests : SimulacronTest
    {
        private const string TableName = "sensor_data";

        private static MappingConfiguration GetSensorDataMappingConfig()
        {
            return new MappingConfiguration().Define(new Map<SensorData>()
                .ExplicitColumns()
                .Column(t => t.Id, cm => cm.WithName("sensor_id"))
                .Column(t => t.Bucket)
                .Column(t => t.Timestamp)
                .Column(t => t.Value)
                .PartitionKey(t => t.Id, t => t.Bucket)
                .ClusteringKey(t => t.Timestamp)
                .TableName(GroupByTests.TableName));
        }

        [Test]
        public void Should_Project_To_New_Anonymous_Type()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT sensor_id, AVG(Value), COUNT(*), SUM(Value), MIN(Value), MAX(Value), Bucket " +
                          $"FROM {GroupByTests.TableName} " +
                          "WHERE sensor_id = ? AND Bucket = ? GROUP BY sensor_id, Bucket",
                          when => when.WithParams("sensor1", "bucket1"))
                      .ThenRowsSuccess(
                          new[]
                          {
                              "sensor_id", "system.avg(value)", "count", "system.sum(value)", "system.min(value)",
                              "system.max(value)", "bucket"
                          },
                          rows => rows.WithRow("sensor1", 2D, 3, 6D, 1.5D, 2.5D, "bucket1")));
            var linqQuery = table
                .GroupBy(t => new { t.Id, t.Bucket })
                .Select(g => new
                {
                    g.Key.Id,
                    Avg = g.Average(i => i.Value),
                    Count = g.Count(),
                    Sum = g.Sum(i => i.Value),
                    Min = g.Min(i => i.Value),
                    Max = g.Max(i => i.Value),
                    g.Key.Bucket
                })
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket1");
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(1, results.Length);
            var aggregation = results[0];
            Assert.AreEqual("sensor1", aggregation.Id);
            Assert.AreEqual("bucket1", aggregation.Bucket);
            Assert.AreEqual(2, aggregation.Avg);
            Assert.AreEqual(3, aggregation.Count);
            Assert.AreEqual(6, aggregation.Sum);
            Assert.AreEqual(1.5, aggregation.Min);
            Assert.AreEqual(2.5, aggregation.Max);
        }

        [Test]
        public void Should_Project_To_Single_Type()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT MAX(Value) FROM {GroupByTests.TableName} GROUP BY sensor_id, Bucket")
                      .ThenRowsSuccess(
                          new[] { "system.max(value)" },
                          rows => rows.WithRow(2.5D).WithRow(3D)));
            var linqQuery = table.GroupBy(t => new { t.Id, t.Bucket }).Select(g => g.Max(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(2, results.Length);
            Assert.AreEqual(2.5, results[0]);
            Assert.AreEqual(3, results[1]);
        }

        [Test]
        public void Should_Project_To_Single_Type_With_Where_Clause()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT MIN(Value) FROM {GroupByTests.TableName} " +
                                 "WHERE sensor_id = ? AND Bucket = ? GROUP BY sensor_id, Bucket",
                          when => when.WithParams("sensor1", "bucket1"))
                      .ThenRowsSuccess(
                          new[] { "system.min(value)" },
                          rows => rows.WithRow(1.5D)));
            var linqQuery = table
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket1")
                .GroupBy(t => new { t.Id, t.Bucket })
                .Select(g => g.Min(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(1, results.Length);
            var min = results[0];
            Assert.AreEqual(1.5, min);
        }

        [Test]
        public void Should_GroupBy_Clustering_Key_With_PK_On_Where_Clause()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT MIN(Value) FROM {GroupByTests.TableName} " +
                                 "WHERE sensor_id = ? AND Bucket = ? GROUP BY Timestamp",
                          when => when.WithParams("sensor1", "bucket2"))
                      .ThenRowsSuccess(
                          new[] { "system.min(value)" },
                          rows => rows.WithRow(1.5D).WithRow(1.0D).WithRow(0.5D)));
            var linqQuery = table
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket2")
                //group by clustering key. PKs are in where clause (no need to be on group by)
                .GroupBy(t => new { t.Timestamp })
                .Select(g => g.Min(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(3, results.Length);
        }

        [Test]
        public void Should_Throw_Exception_When_GroupBy_With_Non_PK_Or_Clustering()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"SELECT MIN(Value) FROM {GroupByTests.TableName} " +
                                 "WHERE sensor_id = ? AND Bucket = ? GROUP BY Value",
                          when => when.WithParams("sensor1", "bucket2"))
                      .ThenServerError(ServerError.Invalid, "msg"));
            var linqQuery = table
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket2")
                .GroupBy(t => new { t.Value })//no clustering key
                .Select(g => g.Min(i => i.Value));
            Assert.Throws<InvalidQueryException>(() => linqQuery.Execute());
        }
    }
}