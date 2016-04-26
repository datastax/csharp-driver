using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra;
using Dse.Graph;
using Dse.Policies;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Graph
{
    [TestFixture]
    class GraphMultiNodeTests : BaseIntegrationTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            CcmHelper.Start(3, new[] { "initial_spark_worker_resources:0.1" }, null, null, true);
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            CcmHelper.Remove();
        }

        [Test]
        public void Should_Contact_Spark_Master_Directly()
        {
            var graphName = "name1";
            using (var cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                session.ExecuteGraph(new SimpleGraphStatement(string.Format("system.graph(\"{0}\")" +
                                                                            ".option(\"graph.schema_mode\").set(com.datastax.bdp.graph.api.model.Schema.Mode.Production)" +
                                                                            ".ifNotExists().create()", graphName)));
                Thread.Sleep(2000); // sleep 2 seconds to allow graph to propagate to all nodes (DSP-9376). 
                session.ExecuteGraph(new SimpleGraphStatement(ClassicSchemaGremlinQuery).SetGraphName(graphName));
                Thread.Sleep(2000);
                session.ExecuteGraph(new SimpleGraphStatement(ClassicLoadGremlinQuery).SetGraphName(graphName));

            }

            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(graphName))
                .WithLoadBalancingPolicy(DseLoadBalancingPolicy.CreateDefault())
                .Build())
            {
                var session = cluster.Connect();

                var sparksRS = session.Execute(new SimpleStatement("call DseClientTool.getAnalyticsGraphServer();"));
                var result = sparksRS.ToArray();
                var sparkLocation = ((SortedDictionary<string, string>)result.First()[0])["location"];
                var sparkHost = sparkLocation.Split(':')[0];

                for (var i = 0; i < 10; i++)
                {
                    var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().count()").SetGraphSourceAnalytics().SetReadTimeoutMillis(120000));
                    Assert.AreEqual(sparkHost, rs.Info.QueriedHost.Address.ToString());
                }
            }
        }
    }
}
