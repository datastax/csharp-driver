using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Dse.Graph;
using Dse.Test.Integration.ClusterManagement;
using NUnit.Framework;
using Newtonsoft.Json;

namespace Dse.Test.Integration.Graph
{
    [TestFixture]
    public class GraphTests : BaseIntegrationTest
    {

        private const string GraphName = "graph1";

        [TestFixtureSetUp]
        public void TestFixtureSetup()
        {
            CcmHelper.Start(1, null, null, null, true);
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
            CreateClassicGraph(CcmHelper.InitialContactPoint, GraphName);
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            CcmHelper.Remove();
        }

        [Test]
        public void Should_Get_Vertices_Of_Classic_Schema()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
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
        public void Should_Retrieve_Graph_Vertices()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko').out('knows')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(2, resultArray.Length);
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
        public void Should_Retrieve_Graph_Edges()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.E().hasLabel('created')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(4, resultArray.Length);
                foreach (Edge edge in resultArray)
                {
                    Assert.NotNull(edge);
                    Assert.AreEqual("created", edge.Label);
                    Assert.True(edge.Properties.ContainsKey("weight"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Support_Named_Parameters()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', namedParam)", new { namedParam = "marko" }));

                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                foreach (Vertex vertex in rs)
                {
                    Assert.NotNull(vertex);
                    Assert.AreEqual("vertex", vertex.Label);
                    Assert.AreEqual("marko", vertex.Properties["name"]);
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Support_List_As_Parameter()
        {
            var names = new[] { "Mario", "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi" };
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var createChars = session.ExecuteGraph(new SimpleGraphStatement("characters.each { character -> " +
                                                                                    "    graph.addVertex(label, 'character', 'name', character);" +
                                                                                "};", new { characters = names }));
                Assert.AreEqual(names.Length, createChars.ToArray().Length);

                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('character').values('name')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(names.Length, resultArray.Length);

                foreach (var name in resultArray)
                {
                    Assert.True(names.Contains(name.ToString()));
                }
            }
        }

        [Test]
        public void Should_Support_Object_As_Parameter()
        {
            var name = "Albert Einstein";
            var year = 1879;
            var field = "Physics";
            var citizenship = new[] { "Kingdom of Württemberg", "Switzerland", "Austria", "Germany", "United States" };

            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();


                session.ExecuteGraph(new SimpleGraphStatement("Vertex scientist = graph.addVertex(label, 'scientist', 'name', m.name, 'year_born', m.year_born, 'field', m.field);" +
                                                                                    " m.citizenship.each { c -> " +
                                                                                    "    Vertex country = graph.addVertex(label, 'country', 'name', c);" +
                                                                                    "    scientist.addEdge('had_citizenship', country);" +
                                                                                    "};", new { m = new { name = name, year_born = year, citizenship = citizenship, field = field } }));


                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('scientist').has('name', name)", new { name = name }));
                Vertex einstein = rs.FirstOrDefault();
                Assert.NotNull(einstein);
                Assert.AreEqual("scientist", einstein.Label);
                Assert.AreEqual(name, einstein.Properties["name"].ToArray()[0].Get<string>("value"));
                Assert.AreEqual(year, einstein.Properties["year_born"].ToArray()[0].Get<int>("value"));
                Assert.AreEqual(field, einstein.Properties["field"].ToArray()[0].Get<string>("value"));

                var citizenships = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('scientist').has('name', name).outE('had_citizenship').inV().values('name');", new { name = name }));
                var citizenshipArray = citizenships.ToArray();
                Assert.AreEqual(citizenship.Length, citizenshipArray.Length);

                foreach (var countryName in citizenshipArray)
                {
                    Assert.True(citizenship.Contains(countryName.ToString()));
                }

            }
        }

        [Test]
        public void Should_Support_Multiple_Named_Parameters()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("[a,b]", new { a = 10, b = 20 }));

                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(2, resultArray.Length);
                Assert.AreEqual(10, resultArray[0].ToInt32());
                Assert.AreEqual(20, resultArray[1].ToInt32());
            }
        }

        [Test]
        public void Should_Handle_Vertex_Id_As_Parameter()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko')"));

                Vertex markoVertex = rs.FirstOrDefault();

                var id = JsonConvert.DeserializeObject(markoVertex.Id.ToString());

                var rsById = session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { vertexId = id }));
                Assert.NotNull(rsById);
                var byIdResultArray = rsById.ToArray();
                Assert.AreEqual(1, byIdResultArray.Length);

                Vertex byIdMarkoVertex = byIdResultArray[0];
                Assert.NotNull(byIdMarkoVertex);
                Assert.AreEqual(markoVertex.Id, byIdMarkoVertex.Id);
                Assert.AreEqual(markoVertex.Label, byIdMarkoVertex.Label);
                Assert.AreEqual(markoVertex.Properties["name"].ToArray()[0].Get<string>("value"), byIdMarkoVertex.Properties["name"].ToArray()[0].Get<string>("value"));
            }
        }

        [Test]
        public void Should_Handle_Edge_Id_As_Parameter()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.E().has('weight', 0.5)"));

                Edge markoKnowsVadasEdge = rs.FirstOrDefault();

                var id = JsonConvert.DeserializeObject(markoKnowsVadasEdge.Id.ToString());

                var rsById = session.ExecuteGraph(new SimpleGraphStatement("g.E(edgeId)", new { edgeId = id }));
                Assert.NotNull(rsById);
                var byIdResultArray = rsById.ToArray();
                Assert.AreEqual(1, byIdResultArray.Length);

                Edge byIdMarkoEdge = byIdResultArray[0];
                Assert.NotNull(byIdMarkoEdge);
                Assert.AreEqual(markoKnowsVadasEdge.Id, byIdMarkoEdge.Id);
                Assert.AreEqual(markoKnowsVadasEdge.Label, byIdMarkoEdge.Label);
                Assert.AreEqual(markoKnowsVadasEdge.Properties["weight"].ToDouble(), byIdMarkoEdge.Properties["weight"].ToDouble());
            }
        }

        [Test]
        public void Should_Retrieve_Path_With_Labels()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('person').has('name', 'marko').as('a')" +
                                                                        ".outE('knows').as('b').inV().as('c', 'd')" +
                                                                        ".outE('created').as('e', 'f', 'g').inV().as('h').path()"));

                var resultArray = rs.ToArray();
                Assert.AreEqual(2, resultArray.Length);

                foreach (var graphResult in resultArray)
                {
                    var labels = graphResult.Get<GraphNode[]>("labels");
                    Assert.AreEqual(5, labels.Length);

                    Assert.AreEqual(1, labels[0].ToArray().Length);
                    Assert.AreEqual("a", labels[0].ToArray()[0].ToString());

                    Assert.AreEqual(1, labels[1].ToArray().Length);
                    Assert.AreEqual("b", labels[1].ToArray()[0].ToString());

                    Assert.AreEqual(2, labels[2].ToArray().Length);
                    Assert.AreEqual("c", labels[2].ToArray()[0].ToString());
                    Assert.AreEqual("d", labels[2].ToArray()[1].ToString());

                    Assert.AreEqual(3, labels[3].ToArray().Length);
                    Assert.AreEqual("e", labels[3].ToArray()[0].ToString());
                    Assert.AreEqual("f", labels[3].ToArray()[1].ToString());
                    Assert.AreEqual("g", labels[3].ToArray()[2].ToString());

                    Assert.AreEqual(1, labels[4].ToArray().Length);
                    Assert.AreEqual("h", labels[4].ToArray()[0].ToString());

                    var objects = graphResult.Get<dynamic[]>("objects");
                    Assert.AreEqual(5, objects.Length);

                    var marko = objects[0];
                    var knows = objects[1];
                    var josh = objects[2];
                    var created = objects[3];
                    var software = objects[4];

                    Assert.AreEqual("person", marko.label);
                    Assert.AreEqual("vertex", marko.type);
                    Assert.AreEqual("marko", marko.properties.name[0].value);
                    Assert.AreEqual(29, marko.properties.age[0].value);

                    Assert.AreEqual("person", josh.label);
                    Assert.AreEqual("vertex", josh.type);
                    Assert.AreEqual("josh", josh.properties.name[0].value);
                    Assert.AreEqual(32, josh.properties.age[0].value);

                    Assert.AreEqual("software", software.label);
                    Assert.AreEqual("vertex", software.type);
                    Assert.AreEqual("java", software.properties.lang[0].value);

                    if (software.properties.name[0].value == "lop")
                    {
                        Assert.AreEqual(0.4, created.properties.weight);
                    }
                    else {
                        Assert.AreEqual(1.0, created.properties.weight);
                        Assert.AreEqual(software.properties.name[0].value, "ripple");
                    }

                    Assert.AreEqual("created", created.label);
                    Assert.AreEqual("edge", created.type);
                    Assert.AreEqual("person", created.outVLabel);
                    Assert.AreEqual("software", created.inVLabel);

                    Assert.AreEqual("knows", knows.label);
                    Assert.AreEqual("edge", knows.type);
                    Assert.AreEqual(1, knows.properties.weight);
                    Assert.AreEqual("person", knows.outVLabel);
                    Assert.AreEqual("person", knows.inVLabel);
                }
            }
        }

        [Test]
        public void Should_Return_Zero_Results()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(CcmHelper.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('notALabel')"));

                var resultArray = rs.ToArray();
                Assert.AreEqual(0, resultArray.Length);
            }
        }
    }
}