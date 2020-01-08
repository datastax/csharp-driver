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
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    public class BatchStatementSimulacron : SimulacronTest
    {
        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_GeneratedTimestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");
            var generator = new MockTimestampGenerator();
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions()))
            using (var cluster = Cluster.Builder()
                                        .AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .WithTimestampGenerator(generator).Build())
            {
                var session = cluster.Connect();
                var batchStatement = new BatchStatement().Add(query);
                session.Execute(batchStatement);
                var timestamp = generator.Next();
                var executed = simulacronCluster.GetQueries(null, QueryType.Batch);
                Assert.IsNotEmpty(executed);
                var executedArray = executed.ToArray();
                Assert.AreEqual(1, executedArray.Length);
                var log = executedArray[0];
                var logtimestamp = log.ClientTimestamp;
                Assert.AreEqual(timestamp, logtimestamp);
            }
        }

        [Test]
        [TestCassandraVersion(2, 1)]
        public void Batch_DefaultGeneratedTimestamp()
        {
            var query = new SimpleStatement($"INSERT INTO {_tableName} (id) values (-99999)");
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions()))
            using (var cluster = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var oldTimestamp = cluster.Configuration.Policies.TimestampGenerator.Next();
                var batchStatement = new BatchStatement().Add(query);
                session.Execute(batchStatement);
                var executed = simulacronCluster.GetQueries(null, QueryType.Batch);
                Assert.IsNotEmpty(executed);
                var executedArray = executed.ToArray();
                Assert.AreEqual(1, executedArray.Length);
                var log = executedArray[0];
                var logtimestamp = log.ClientTimestamp;
                Assert.Greater(logtimestamp, oldTimestamp);
            }
        }

        private readonly string _tableName = "tbl" + Guid.NewGuid().ToString("N").ToLower();
    }
}