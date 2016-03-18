using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Dse.Graph;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;

namespace Dse.Test.Integration.Graph
{
    public class GraphTests : BaseIntegrationTest
    {
        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            CcmHelper.Start(1, null, null, null, true);
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            CcmHelper.Remove();
        }

        [Test]
        public void Should_Execute_Simple_Graph_Query()
        {
            using (var cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                //create graph1
                session.ExecuteGraph(new SimpleGraphStatement("system.createGraph('graph1').ifNotExist().build()"));
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetGraphName("graph1"));
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Get_Vertices_Of_Classic_Schema()
        {
            CreateClassicGraph(CcmHelper.InitialContactPoint, "classic1");
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName("classic1"))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
                var resultArray = rs.ToArray();
                Assert.Greater(resultArray.Length, 0);
                foreach (Vertex v in rs)
                {
                    Assert.NotNull(v);
                    Assert.AreEqual("vertex", v.Label);
                    Assert.True(v.Properties.ContainsKey("name"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Get_Edge_By_Parameters()
        {
            CreateClassicGraph(CcmHelper.InitialContactPoint, "classic2");
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName("classic2"))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.E().hasLabel(myLabel)", new { myLabel = "created" }));
                var resultArray = rs.ToArray();
                Assert.Greater(resultArray.Length, 0);
                foreach (Edge edge in resultArray)
                {
                    Assert.NotNull(edge.Label);
                    Assert.Greater(edge.Properties["weight"].ToDouble(), 0);
                }
                rs = session.ExecuteGraph(new SimpleGraphStatement(new Dictionary<string, object> { { "myLabel", "created" } }, "g.E().hasLabel(myLabel)"));
                Assert.Greater(rs.Count(), 0);
            }
        }
    }
}
