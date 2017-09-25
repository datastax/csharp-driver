
using System;
using System.Dynamic;
using System.Linq;
using Cassandra.IntegrationTests.TestClusterManagement;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SCluster = Cassandra.IntegrationTests.TestClusterManagement.Simulacron.Cluster;

namespace Cassandra.IntegrationTests.Core
{
    [Category("simulacron"), TestFixture]
    public class SimulacronHealthTest
    {
        [SetUp]
        public void Setup()
        {
            SimulacronManager.Instance.Start();
        }
        
        [TearDown]
        public void TestTearDown()
        {
            SimulacronManager.Instance.Stop();
        }

        [Test]
        public void Should__Create_Cluster()
        {
            var simulacronCluster = SCluster.Create("3", "3.10", "test", true, 1);
            var contactPoint = simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint.Item1)
                .WithPort(contactPoint.Item2);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();
                
                dynamic primeQuery = new JObject();
                primeQuery.when = new JObject();
                primeQuery.when.query = "SELECT * FROM system.traces";
                primeQuery.then = new JObject();
                primeQuery.then.result = "success";
                primeQuery.then.delay_in_ms = 0;
                var rows = new JArray();
                primeQuery.then.rows = rows;
                dynamic row = new JObject();
                row.id = Guid.NewGuid();
                row.value = "value";
                rows.Add(row);
                primeQuery.then.column_types = new JObject();
                primeQuery.then.column_types.id = "uuid";
                primeQuery.then.column_types.value = "varchar";
                
                simulacronCluster.Prime(primeQuery);
                var result = session.Execute("SELECT * FROM system.traces");
                var firstRow = result.FirstOrDefault();
                Assert.NotNull(firstRow);
                Assert.AreEqual("value", firstRow["value"]);

                var logs = simulacronCluster.GetLogs();
                TestContext.WriteLine(logs.ToString());
                var executed = false;
                var dcLogs = logs["data_centers"][0];
                var nodes = dcLogs["nodes"] as JArray;
                foreach (var node in nodes)
                {
                    var queries = node["queries"] as JArray;
                    foreach (var query in queries)
                    {
                        if (query["query"].ToString() == "SELECT * FROM system.traces")
                        {
                            executed = true;
                        }
                    }
                }
                Assert.True(executed);

            }   
        }
    }
}
