
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SCluster = Cassandra.IntegrationTests.TestClusterManagement.Simulacron.Cluster;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short"), TestFixture]
    public class SimulacronHealthTest
    {
        [Test]
        public void Should_CreateSimulacronCluster()
        {
            const string query = "SELECT * FROM system.traces";
            var simulacronCluster = SCluster.Create("3", "3.10", "test", true, 1);
            var contactPoint = simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint.Item1)
                .WithPort(contactPoint.Item2);
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
