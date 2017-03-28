//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading;
using Dse.Geometry;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Integration.Graph
{
    [TestFixture, TestDseVersion(5, 0)]
    public class GraphTests : BaseIntegrationTest
    {
        private const string GraphName = "graph1";
        private int _idGenerator;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            TestClusterManager.CreateNew(1, new TestClusterOptions { Workloads = new [] { "graph" } });
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(20000);
            CreateClassicGraph(TestClusterManager.InitialContactPoint, GraphName);
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            TestClusterManager.TryRemove();
        }

        [Test]
        public void Should_Parse_Dse_Workload()
        {
            TestUtils.VerifyCurrentClusterWorkloads(DseVersion >= Version.Parse("5.1")
                ? new[] { "Cassandra", "Graph" }
                : new[] { "Cassandra"});
        }

        [Test]
        public void Should_Get_Vertices_Of_Classic_Schema()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
                var resultArray = rs.ToArray();
                Assert.Greater(resultArray.Length, 0);
                foreach (Vertex v in resultArray)
                {
                    Assert.NotNull(v);
                    Assert.IsTrue(v.Label == "person" || v.Label == "software");
                    Assert.True(v.Properties.ContainsKey("name"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Retrieve_Graph_Vertices()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko').out('knows')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(2, resultArray.Length);
                foreach (Vertex v in resultArray)
                {
                    Assert.NotNull(v);
                    Assert.AreEqual("person", v.Label);
                    Assert.True(v.Properties.ContainsKey("name"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Retrieve_Graph_Edges()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
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
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', namedParam)", new { namedParam = "marko" }));

                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var vertex = resultArray[0].ToVertex();
                Assert.NotNull(vertex);
                Assert.AreEqual("person", vertex.Label);
                Assert.AreEqual("marko", vertex.Properties["name"].ToArray()[0].Get<string>("value"));
            }
        }

        [Test]
        public void Should_Support_List_As_Parameter()
        {
            var names = new[] { "Mario", "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi" };
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var schemaCharacterQuery = "" +
                                   "schema.propertyKey(\"characterName\").Text().create();\n" +
                                   "schema.vertexLabel(\"character\").properties(\"characterName\").create();";

                session.ExecuteGraph(new SimpleGraphStatement(schemaCharacterQuery));
                var createChars = session.ExecuteGraph(new SimpleGraphStatement("characters.each { character -> " +
                                                                                    "    graph.addVertex(label, 'character', 'characterName', character);" +
                                                                                "};", new { characters = names }));
                Assert.AreEqual(names.Length, createChars.ToArray().Length);

                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('character').values('characterName')"));
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
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();

                var schemaScientistQuery = "" +
                      "schema.propertyKey(\"year_born\").Int().create()\n" +
                      "schema.propertyKey(\"field\").Text().create()\n" +
                      "schema.propertyKey(\"scientist_name\").Text().create()\n" +
                      "schema.propertyKey(\"country_name\").Text().create()\n" +
                      "schema.vertexLabel(\"scientist\").properties(\"scientist_name\", \"year_born\", \"field\").create()\n" +
                      "schema.vertexLabel(\"country\").properties(\"country_name\").create()\n" +
                      "schema.edgeLabel(\"had_citizenship\").connection(\"scientist\", \"country\").create()";
                session.ExecuteGraph(new SimpleGraphStatement(schemaScientistQuery));

                session.ExecuteGraph(new SimpleGraphStatement("Vertex scientist = graph.addVertex(label, 'scientist', 'scientist_name', m.name, 'year_born', m.year_born, 'field', m.field);" +
                                                                                    " m.citizenship.each { c -> " +
                                                                                    "    Vertex country = graph.addVertex(label, 'country', 'country_name', c);" +
                                                                                    "    scientist.addEdge('had_citizenship', country);" +
                                                                                    "};", new { m = new { name = name, year_born = year, citizenship = citizenship, field = field } }));


                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('scientist').has('scientist_name', name)", new { name = name }));
                Vertex einstein = rs.FirstOrDefault();
                Assert.NotNull(einstein);
                Assert.AreEqual("scientist", einstein.Label);
                Assert.AreEqual(name, einstein.Properties["scientist_name"].ToArray()[0].Get<string>("value"));
                Assert.AreEqual(year, einstein.Properties["year_born"].ToArray()[0].Get<int>("value"));
                Assert.AreEqual(field, einstein.Properties["field"].ToArray()[0].Get<string>("value"));

                var citizenships = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('scientist').has('scientist_name', name).outE('had_citizenship').inV().values('country_name');", new { name = name }));
                var citizenshipArray = citizenships.ToArray();
                Assert.AreEqual(citizenship.Length, citizenshipArray.Length);

                foreach (var countryName in citizenshipArray)
                {
                    Assert.True(citizenship.Contains(countryName.ToString()));
                }

            }
        }

        [Test]
        public void Should_Support_Dictionary_As_Parameter()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var parameter = new Dictionary<string, object>();
                parameter.Add("namedParam", "marko");
                var rs = session.ExecuteGraph(new SimpleGraphStatement(parameter, "g.V().has('name', namedParam)"));

                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var vertex = resultArray[0].ToVertex();
                Assert.NotNull(vertex);
                Assert.AreEqual("person", vertex.Label);
                Assert.AreEqual("marko", vertex.Properties["name"].ToArray()[0].Get<string>("value"));
            }
        }

        [Test]
        public void Should_Support_Multiple_Named_Parameters()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
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
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko')"));

                Vertex markoVertex = rs.FirstOrDefault();

                var rsById = session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { vertexId = markoVertex.Id }));
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
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.E().has('weight', 0.5)"));

                Edge markoKnowsVadasEdge = rs.FirstOrDefault();

                var rsById = session.ExecuteGraph(new SimpleGraphStatement("g.E(edgeId)", new { edgeId = markoKnowsVadasEdge.Id }));
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
                .AddContactPoint(TestClusterManager.InitialContactPoint)
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

                    var path = graphResult.ToPath();
                    var objects = path.Objects.ToList();
                    Assert.AreEqual(5, objects.Count);

                    var marko = objects[0].ToVertex();
                    var knows = objects[1].ToEdge();
                    var josh = objects[2].ToVertex();
                    var created = objects[3].ToEdge();
                    var software = objects[4].ToVertex();

                    Assert.AreEqual("person", marko.Label);
                    Assert.AreEqual("marko", marko.Properties["name"].ToArray()[0].Get<string>("value"));
                    Assert.AreEqual(29, marko.Properties["age"].ToArray()[0].Get<int>("value"));

                    Assert.AreEqual("person", josh.Label);
                    Assert.AreEqual("josh", josh.Properties["name"].ToArray()[0].Get<string>("value"));
                    Assert.AreEqual(32, josh.Properties["age"].ToArray()[0].Get<int>("value"));

                    Assert.AreEqual("software", software.Label);
                    Assert.AreEqual("java", software.Properties["lang"].ToArray()[0].Get<string>("value"));

                    if (software.Properties["name"].ToArray()[0].Get<string>("value") == "lop")
                    {
                        Assert.AreEqual(0.4, created.Properties["weight"].ToDouble());
                    }
                    else {
                        Assert.AreEqual(1.0, created.Properties["weight"].ToDouble());
                        Assert.AreEqual("ripple", software.Properties["name"].ToArray()[0].Get<string>("value"));
                    }

                    Assert.AreEqual("created", created.Label);
                    Assert.AreEqual("person", created.OutVLabel);
                    Assert.AreEqual("software", created.InVLabel);

                    Assert.AreEqual("knows", knows.Label);
                    Assert.AreEqual(1, knows.Properties["weight"].ToDouble());
                    Assert.AreEqual("person", knows.OutVLabel);
                    Assert.AreEqual("person", knows.InVLabel);
                }
            }
        }

        [Test]
        public void Should_Return_Zero_Results()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('notALabel')"));

                var resultArray = rs.ToArray();
                Assert.AreEqual(0, resultArray.Length);
            }
        }

        [Test]
        public void Should_Have_The_Same_ReadTimeout_Per_Statement_And_Global()
        {
            const int timeout = 2000;
            const int timeoutThreshold = timeout / 10; //10%
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName).SetReadTimeoutMillis(timeout))
                .Build())
            {
                var session = cluster.Connect();
                var stopwatch = new Stopwatch();
                try
                {
                    stopwatch.Start();
                    session.ExecuteGraph(new SimpleGraphStatement("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(3000L);"));
                }
                catch
                {
                    stopwatch.Stop();
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeout - timeoutThreshold);
                }
            }
        }

        [Test]
        public void Should_Have_The_Different_ReadTimeout_Per_Statement()
        {
            const int timeout = 2000;
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName).SetReadTimeoutMillis(timeout))
                .Build())
            {
                var session = cluster.Connect();
                var stopwatch = new Stopwatch();
                const int stmtTimeout = 500;
                const int stmtTimeoutThreshold = stmtTimeout / 10; //10%
                try
                {
                    stopwatch.Start();
                    session.ExecuteGraph(new SimpleGraphStatement("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(1000L);")
                                            .SetReadTimeoutMillis(stmtTimeout));
                }
                catch
                {
                    stopwatch.Stop();
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, stmtTimeout - stmtTimeoutThreshold);
                    Assert.Less(stopwatch.ElapsedMilliseconds, timeout);
                }
            }
        }

        [Test]
        public void Should_Have_Infinite_ReadTimeout_Per_Statement()
        {
            //setting a really small global timeout to make sure that the query exceeds this time
            const int timeout = 2000;
            const int timeoutThreshold = timeout / 10; //10%
            const long stmtSleep = 10000L;
            const long stmtSleepThreashold = stmtSleep / 10; //10%
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName).SetReadTimeoutMillis(timeout))
                .Build())
            {
                var session = cluster.Connect();
                var stopwatch = new Stopwatch();
                try
                {
                    stopwatch.Start();
                    session.ExecuteGraph(new SimpleGraphStatement("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(" + stmtSleep + ");")
                                                                        .SetReadTimeoutMillis(Timeout.Infinite));
                }
                finally
                {
                    stopwatch.Stop();
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeout - timeoutThreshold);
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, stmtSleep - stmtSleepThreashold);
                }
            }
        }

        [Test]
        public void Should_Get_Path_With_Labels()
        {
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.V().hasLabel('person').has('name', 'marko').as('a').outE('knows').as('b').inV().as('c', 'd')" +
                    ".outE('created').as('e', 'f', 'g').inV().as('h').path()"));
                foreach (Path path in rs)
                {
                    CollectionAssert.AreEqual(
                        new string[][]
                        {
                            new [] { "a" }, new [] {"b"}, new[] { "c", "d" }, new[] { "e", "f", "g" }, new [] { "h" }
                        }, path.Labels);
                    var person = path.Objects.First().ToVertex();
                    Assert.AreEqual("person", person.Label);
                    Assert.True(person.Properties.ContainsKey("name"));
                }
            }
        }

        [TestCase("Boolean()", true, "True")]
        [TestCase("Boolean()", false, "False")]
        [TestCase("Int()", int.MaxValue, "2147483647")]
        [TestCase("Int()", int.MinValue, "-2147483648")]
        [TestCase("Int()", 0, "0")]
        [TestCase("Smallint()", short.MaxValue, "32767")]
        [TestCase("Smallint()", -short.MinValue, "-32768")]
        [TestCase("Smallint()", 0, "0")]
        [TestCase("Bigint()", long.MaxValue, "9223372036854775807")]
        [TestCase("Bigint()", long.MinValue, "-9223372036854775808")]
        [TestCase("Bigint()", 0L, "0")]
        [TestCase("Float()", 3.1415927f, "3.1415927")]
        [TestCase("Double()", 3.1415d, "3.1415")]
        [TestCase("Duration()", "5 s", "PT5S")]
        [TestCase("Duration()", "5 seconds", "PT5S")]
        [TestCase("Duration()", "1 minute", "PT1M")]
        [TestCase("Duration()", "PT1H1M", "PT1H1M")]
        [TestCase("Duration()", "PT240H", "PT240H")]
        [TestCase("Text()", "The quick brown fox jumps over the lazy dog", "The quick brown fox jumps over the lazy dog")]
        public void Should_Support_Types(string type, object value, string expectedString)
        {
            var id = _idGenerator++;
            var vertexLabel = "vertex" + id;
            var propertyName = "prop" + id;
            IncludeAndQueryVertex(vertexLabel, propertyName, type, value, expectedString);
        }

        private Vertex IncludeAndQueryVertex(string vertexLabel, string propertyName, string type, object value, string expectedString, bool verifyToString = true)
        {
            Vertex vertex = null;
            using (var cluster = DseCluster.Builder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(GraphName))
                .Build())
            {
                var session = cluster.Connect();

                var schemaQuery = string.Format("schema.propertyKey(propertyName).{0}.ifNotExists().create();\n", type) +
                                  "schema.vertexLabel(vertexLabel).properties(propertyName).ifNotExists().create();";

                session.ExecuteGraph(new SimpleGraphStatement(schemaQuery, new { vertexLabel = vertexLabel, propertyName = propertyName }));

                var parameters = new { vertexLabel = vertexLabel, propertyName = propertyName, val = value };
                session.ExecuteGraph(new SimpleGraphStatement("g.addV(label, vertexLabel, propertyName, val)", parameters));


                var rs =
                    session.ExecuteGraph(
                        new SimpleGraphStatement("g.V().hasLabel(vertexLabel).has(propertyName, val).next()", parameters));
                var first = rs.FirstOrDefault();
                Assert.NotNull(first);
                vertex = first.ToVertex();
                if (verifyToString)
                {
                    ValidateVertexResult(vertex, vertexLabel, propertyName, expectedString);
                }
            }
            return vertex;
        }

        private void TestInsertSelectProperty<T>(string type, T value, bool verifyToString = true)
        {
            var id = _idGenerator++;
            var vertexLabel = "vertex" + id;
            var propertyName = "prop" + id;
            var vertex = IncludeAndQueryVertex(vertexLabel, propertyName, type, value, value.ToString(), verifyToString);
            var propObject = vertex.Properties[propertyName].ToArray()[0].Get<T>("value");
            Assert.AreEqual(value, propObject);
        }

        [Test]
        public void Should_Support_Point()
        {
            var type = "Point()";
            if (TestClusterManager.DseVersion >= Version.Parse("5.1.0"))
            {
                type = "Point().withBounds(-40, -40, 40, 40)";
            }
            var point = new Point(0, 1);
            TestInsertSelectProperty(type, point);
        }

        [Test]
        public void Should_Support_Line()
        {
            var type = "Linestring()";
            if (TestClusterManager.DseVersion >= Version.Parse("5.1.0"))
            {
                type = "Linestring().withGeoBounds()";
            }
            var lineString = new LineString(new Point(0, 0), new Point(0, 1), new Point(1, 1));
            TestInsertSelectProperty(type, lineString);
        }

        [Test]
        public void Should_Support_Polygon()
        {
            var type = "Polygon()";
            if (TestClusterManager.DseVersion >= Version.Parse("5.1.0"))
            {
                type = "Polygon().withGeoBounds()";
            }
            var polygon = new Polygon(new Point(-10, 10), new Point(10, 0), new Point(10, 10), new Point(-10, 10));
            TestInsertSelectProperty(type, polygon);
        }

        [Test]
        public void Should_Support_Inet()
        {
            var address = IPAddress.Parse("127.0.0.1");
            TestInsertSelectProperty("Inet()", address);
        }

        [Test]
        public void Should_Support_Guid()
        {
            var guid = Guid.NewGuid();
            TestInsertSelectProperty("Uuid()", guid);
        }

        [Test]
        public void Should_Support_Decimal()
        {
            var type = "Decimal()";
            var decimalValue = 10.10M;
            TestInsertSelectProperty(type, decimalValue, false);
        }

        [Test]
        public void Should_Support_BigInteger()
        {
            var type = "Varint()";
            var varint = BigInteger.Parse("8675309");
            TestInsertSelectProperty(type, varint);
        }

        [Test]
        public void Should_Support_Timestamp()
        {
            var type = "Timestamp()";
            var timestamp = DateTimeOffset.Parse("2016-02-04T02:26:31.657Z");
            TestInsertSelectProperty(type, timestamp, false);
        }

        [Test]
        public void Should_Support_Duration()
        {
            var type = "Duration()";
            var value = new Duration(0, 0, 61000000000L);
            TestInsertSelectProperty(type, value, false);
        }

        [Test]
        public void ExecuteGraph_Should_Throw_ArgumentOutOfRange_When_Duration_Is_Out_Of_Range()
        {
            var values = new[]
            {
                new Duration(1, 0, 0),
                new Duration(-1, 0, 0)
            };
            using (var cluster = DseCluster.Builder()
                                           .AddContactPoint(TestClusterManager.InitialContactPoint)
                                           .WithGraphOptions(new GraphOptions().SetName(GraphName))
                                           .Build())
            {
                var session = cluster.Connect();
                foreach (var value in values)
                {

                    var parameters = new {vertexLabel = "v1", propertyName = "prop1", val = value};
                    var stmt = new SimpleGraphStatement("g.addV(label, vertexLabel, propertyName, val)", parameters);
                    Assert.Throws<ArgumentOutOfRangeException>(() => session.ExecuteGraph(stmt));
                }
            }
        }

        private void ValidateVertexResult(Vertex vertex, string vertexLabel, string propertyName, string expectedValueString)
        {
            Assert.AreEqual(vertex.Label, vertexLabel);
            var valueString = vertex.Properties[propertyName].ToArray()[0].Get<string>("value");
            Assert.AreEqual(expectedValueString, valueString);
        }
    }
}
