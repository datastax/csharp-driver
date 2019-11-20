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
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Data.Linq;
using System.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), Category("realcluster"), TestCassandraVersion(3, 10)]
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
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket1', now(), 2.5)",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket2', now(), 1)",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket2', now(), 1.5)",
                    "INSERT INTO sensor_data (sensor_id, bucket, timestamp, value) VALUES ('sensor1', 'bucket2', now(), 0.5)"
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
            var linqQuery = table.GroupBy(t => new {t.Id, t.Bucket}).Select(g => g.Max(i => i.Value));
            var results = linqQuery.Execute().ToArray();
            Assert.AreEqual(2, results.Length);
            var max = results[0];
            Assert.AreEqual(2.5, max);
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
            Assert.AreEqual(1.5, min);
        }

        [Test]
        public void Should_GroupBy_Clustering_Key_With_PK_On_Where_Clause()
        {
            var table = new Table<SensorData>(Session, GetSensorDataMappingConfig());
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
            var linqQuery = table
                .Where(t => t.Id == "sensor1" && t.Bucket == "bucket2")
                .GroupBy(t => new { t.Value })//no clustering key
                .Select(g => g.Min(i => i.Value));
            Assert.Throws<InvalidQueryException>(() => linqQuery.Execute());
        }
    }
}
