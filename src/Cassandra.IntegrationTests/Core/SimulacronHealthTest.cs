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
//

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short"), TestFixture]
    public class SimulacronHealthTest
    {
        [Test]
        public void Should_CreateSimulacronCluster()
        {
            const string query = "SELECT * FROM system.traces";
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" } );
            var contactPoint = simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();

                var primeQuery = new
                {
                    when = new { query = query },
                    then = new
                    {
                        result = "success", 
                        delay_in_ms = 0,
                        rows = new []
                        {
                            new
                            {
                                id = Guid.NewGuid(),
                                value = "value"
                            }
                        },
                        column_types = new
                        {
                            id = "uuid",
                            value = "varchar"
                        }
                    }
                };

                simulacronCluster.Prime(primeQuery);
                var result = session.Execute(query);
                var firstRow = result.FirstOrDefault();
                Assert.NotNull(firstRow);
                Assert.AreEqual("value", firstRow["value"]);

                var logs = simulacronCluster.GetLogs();
                var dcLogs = logs.data_centers as IEnumerable<dynamic>;
                Assert.NotNull(dcLogs);
                Assert.True(
                    dcLogs.Any(dc =>
                        (dc.nodes as IEnumerable<dynamic>).Any(node => 
                            (node.queries as IEnumerable<dynamic>).Any(q => 
                                q.query.ToString() == query)))
                    );
            }   
        }
    }
}
