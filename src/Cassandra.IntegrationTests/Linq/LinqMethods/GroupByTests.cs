using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Data.Linq;
using System.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), TestCassandraVersion(3, 10)]
    public class GroupByTests : SharedClusterTest
    {
        protected override string[] SetupQueries
        {
            get
            {
                return new []
                {
                    "CREATE TABLE sensor_data (sensor_id text, bucket text, timestamp timeuuid, value double," +
                    " PRIMARY KEY ((sensor_id, bucket), timestamp))",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket1', now(), 1.5)",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket1', now(), 2)",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket1', now(), 2.5)"
                };
            }
        }

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
                .TableName("sensor_data"));
        }

        [Test]
        public void Should_Project_To_New_Anonymous_Type()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            var linqQuery = table
                .GroupBy(t => new {t.Id, t.Bucket})
                .Select(g => new
                {
                    g.Key.Id,
                    Avg = g.Average(i => i.Value),
                    Count = g.Count(),
                    Sum = g.Sum(i => i.Value),
                    Min = g.Min(i => i.Value),
                    Max = g.Max(i => i.Value),
                    g.Key.Bucket
                });
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(1, results.Length);
            var aggregation = results[0];
            Assert.AreEqual("sensor1", aggregation.Id);
            Assert.AreEqual("bucket1", aggregation.Bucket);
            Assert.AreEqual(aggregation.Avg, 2);
            Assert.AreEqual(aggregation.Count, 3);
            Assert.AreEqual(aggregation.Sum, 6);
            Assert.AreEqual(aggregation.Min, 1.5);
            Assert.AreEqual(aggregation.Max, 2.5);
        }

        [Test]
        public void Should_Project_To_Single_Type()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            var linqQuery = table.GroupBy(t => new {t.Id, t.Bucket}).Select(g => g.Max(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(1, results.Length);
            var max = results[0];
            Assert.AreEqual(max, 2.5);
        }

        [Test]
        public void Should_Project_To_Single_Type_With_Where_Clause()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
            var linqQuery = table
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket1")
                .GroupBy(t => new {t.Id, t.Bucket})
                .Select(g => g.Min(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(1, results.Length);
            var min = results[0];
            Assert.AreEqual(min, 1.5);
        }
    }
}
