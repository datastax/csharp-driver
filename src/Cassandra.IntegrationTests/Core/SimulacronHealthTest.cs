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
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), TestFixture]
    public class SimulacronHealthTest
    {
        [Test]
        public void Should_CreateSimulacronCluster()
        {
            const string query = "SELECT * FROM system.traces";
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" });
            var contactPoint = simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();

                simulacronCluster.PrimeFluent(b =>
                    b.WhenQuery(query)
                     .ThenRowsSuccess(
                         new[] { ("id", DataType.Uuid), ("value", DataType.Varchar) },
                         rows => rows.WithRow(Guid.NewGuid(), "value")));

                var result = session.Execute(query);
                var firstRow = result.FirstOrDefault();
                Assert.NotNull(firstRow);
                Assert.AreEqual("value", firstRow["value"]);

                var logs = simulacronCluster.GetLogs();
                var dcLogs = logs.DataCenters;
                Assert.NotNull(dcLogs);
                Assert.True(logs.HasQueryBeenExecuted(query));
            }
        }
    }
}