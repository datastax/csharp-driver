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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.DataStax.Graph;
using Cassandra.Geometry;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestDataTypes;
using Cassandra.Tests;
using Cassandra.Tests.TestHelpers;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.DataStax.Graph
{
    [TestDseVersion(6, 8), Category(TestCategory.Short), Category(TestCategory.ServerApi), Category(TestCategory.RealCluster)]
    public class CoreGraphTests : BaseIntegrationTest
    {
        private const string GraphName = "graph2";
        private const string GremlinGroovy = "gremlin-groovy";
        private const string BytecodeJson = "bytecode-json";
        private int _idGenerator;
        private ICluster _cluster;
        private ISession _session;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            TestClusterManager.CreateNew(1, new TestClusterOptions { Workloads = new[] { "graph" } });
            CreateCoreGraph(TestClusterManager.InitialContactPoint, CoreGraphTests.GraphName);
            _cluster = ClusterBuilder()
                                    .AddContactPoint(TestClusterManager.InitialContactPoint)
                                    .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                    .Build();
            _session = _cluster.Connect();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _cluster.Shutdown();
            TestClusterManager.TryRemove();
        }

        [Test]
        public void Should_Parse_Dse_Workload()
        {
            TestUtils.VerifyCurrentClusterWorkloads(TestClusterManager.CheckDseVersion(Version.Parse("5.1"), Comparison.GreaterThanOrEqualsTo)
                ? new[] { "Cassandra", "Graph" }
                : new[] { "Cassandra" });
        }

        [Test]
        public void Should_Retrieve_Graph_Vertices()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko').out('knows')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(2, resultArray.Length);
                foreach (Vertex v in resultArray)
                {
                    Assert.NotNull(v);
                    FillVertexProperties(session, v);
                    Assert.AreEqual("person", v.Label);
                    Assert.True(v.Properties.ContainsKey("name"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Retrieve_Graph_Edges()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.E().hasLabel('created')"));
                var resultArray = rs.To<IEdge>().ToArray();
                Assert.AreEqual(4, resultArray.Length);
                foreach (var edge in resultArray)
                {
                    Assert.NotNull(edge);
                    FillEdgeProperties(session, edge);
                    Assert.AreEqual("created", edge.Label);
                    Assert.NotNull(edge.GetProperty("weight"));
                }
                Assert.NotNull(rs);
            }
        }

        [Test]
        public void Should_Retrieve_Graph_Multiple_Properties()
        {
            using (var cluster = ClusterBuilder()
                                           .AddContactPoint(TestClusterManager.InitialContactPoint)
                                           .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                           .Build())
            {
                var session = cluster.Connect();
                session.ExecuteGraph(new SimpleGraphStatement(
                    "g.addV('movie').property('title', 'Star Wars').property('tags', ['science-fiction', 'adventure'])"));
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('title', 'Star Wars')"));
                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var starWarsVertex = resultArray[0].ToVertex();
                FillVertexProperties(session, starWarsVertex);
                Assert.AreEqual("Star Wars", starWarsVertex.GetProperty("title").Value.ToString());
                var tags = starWarsVertex.GetProperties("tags").SelectMany(p => p.Value.To<IEnumerable<string>>());
                var expectedTags = new List<string>() { "science-fiction", "adventure" };
                CollectionAssert.AreEquivalent(expectedTags, tags);
            }
        }

        [Test]
        public void Should_Support_Named_Parameters()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', namedParam)", new { namedParam = "marko" }));

                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var vertex = resultArray[0].To<IVertex>();
                Assert.NotNull(vertex);
                Assert.AreEqual("person", vertex.Label);
                FillVertexProperties(session, vertex);
                Assert.AreEqual("marko", vertex.GetProperty("name").Value.ToString());
            }
        }

        [Test]
        public void Should_Support_List_As_Parameter()
        {
            var names = new[] { "Mario", "Luigi", "Toad", "Bowser", "Peach", "Wario", "Waluigi" };
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var schemaCharacterQuery = "schema.vertexLabel(\"character\").partitionBy(\"characterName\", Text).create();";

                session.ExecuteGraph(new SimpleGraphStatement(schemaCharacterQuery));
                var createChars = session.ExecuteGraph(new SimpleGraphStatement("characters.each { character -> " +
                                                                                    "    g.addV('character').property('characterName', character).next();" +
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

            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();

                var schemaScientistQuery = "" +
                      "schema.vertexLabel(\"scientist\").partitionBy(\"scientist_name\", Text).property(\"year_born\", Int).property(\"field\", Text).create()\n" +
                      "schema.vertexLabel(\"country\").partitionBy(\"country_name\", Text).create()\n" +
                      "schema.edgeLabel(\"had_citizenship\").from(\"scientist\").to(\"country\").create()";
                session.ExecuteGraph(new SimpleGraphStatement(schemaScientistQuery));

                session.ExecuteGraph(new SimpleGraphStatement(
                    "Vertex scientist1 = g.addV('scientist').property('scientist_name', m.name).property('year_born', m.year_born).property('field', m.field).next();" +
                    "m.citizenship.each { c -> " +
                    "  Vertex country1 = g.addV('country').property('country_name', c).next();" +
                    "  g.addE('had_citizenship').from(scientist1).to(country1).next();" +
                    "};", new { m = new { name, year_born = year, citizenship, field } }));

                var rs = session.ExecuteGraph(
                    new SimpleGraphStatement("g.V().hasLabel('scientist').has('scientist_name', name)", new { name }));
                Vertex einstein = rs.FirstOrDefault();
                Assert.NotNull(einstein);
                Assert.AreEqual("scientist", einstein.Label);
                FillVertexProperties(session, einstein);
                // Vertices contain an array of values per each property
                Assert.AreEqual(new[] { true, true, true },
                                new[] { "scientist_name", "year_born", "field" }.Select(propName =>
                                      einstein.Properties[propName].IsArray));
                Assert.AreEqual(name, einstein.GetProperty("scientist_name").Value.ToString());
                Assert.AreEqual(year, einstein.GetProperty("year_born").Value.To<int>());
                Assert.AreEqual(field, einstein.GetProperty("field").Value.ToString());

                var citizenships = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.V().hasLabel('scientist').has('scientist_name', name)" +
                    ".outE('had_citizenship').inV().values('country_name')", new { name }));
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
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var parameter = new Dictionary<string, object>();
                parameter.Add("namedParam", "marko");
                var rs = session.ExecuteGraph(new SimpleGraphStatement(parameter, "g.V().has('name', namedParam)"));

                Assert.NotNull(rs);
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var vertex = resultArray[0].To<IVertex>();
                Assert.NotNull(vertex);
                Assert.AreEqual("person", vertex.Label);
                FillVertexProperties(session, vertex);
                Assert.AreEqual("marko", vertex.GetProperty("name").Value.ToString());
            }
        }

        [Test]
        public void Should_Support_Multiple_Named_Parameters()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
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
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement("g.V().has('name', 'marko')"));

                var markoVertex = rs.To<IVertex>().First();

                var rsById = session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { vertexId = markoVertex.Id }));
                Assert.NotNull(rsById);
                var byIdResultArray = rsById.ToArray();
                Assert.AreEqual(1, byIdResultArray.Length);

                IVertex byIdMarkoVertex = (Vertex)byIdResultArray[0];
                Assert.NotNull(byIdMarkoVertex);
                Assert.AreEqual(markoVertex.Id, byIdMarkoVertex.Id);
                Assert.AreEqual(markoVertex.Label, byIdMarkoVertex.Label);

                Assert.AreEqual(0, byIdMarkoVertex.GetProperties().Count());
                Assert.AreEqual(0, markoVertex.GetProperties().Count());

                FillVertexProperties(session, byIdMarkoVertex);
                FillVertexProperties(session, markoVertex);

                Assert.AreEqual(markoVertex.GetProperty("name").Value.ToString(),
                    byIdMarkoVertex.GetProperty("name").Value.ToString());
                Assert.AreEqual(markoVertex.GetProperties("name").First().Value.ToString(),
                    byIdMarkoVertex.GetProperties("name").First().Value.ToString());
            }
        }

        [Test]
        public void Should_Handle_Edge_Id_As_Parameter()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(
                    new SimpleGraphStatement("g.with('allow-filtering').E().hasLabel('knows').has('weight', 0.5f)"));

                var markoKnowsVadasEdge = rs.To<IEdge>().First();

                var rsById = session.ExecuteGraph(
                    new SimpleGraphStatement("g.E(edgeId)", new { edgeId = markoKnowsVadasEdge.Id }));
                Assert.NotNull(rsById);
                var byIdResultArray = rsById.ToArray();
                Assert.AreEqual(1, byIdResultArray.Length);

                IEdge byIdMarkoEdge = (Edge)byIdResultArray[0];
                Assert.NotNull(byIdMarkoEdge);
                Assert.AreEqual(markoKnowsVadasEdge.Id, byIdMarkoEdge.Id);
                Assert.AreEqual(markoKnowsVadasEdge.Label, byIdMarkoEdge.Label);

                Assert.AreEqual(0, markoKnowsVadasEdge.GetProperties().Count());
                Assert.AreEqual(0, byIdMarkoEdge.GetProperties().Count());

                FillEdgeProperties(session, byIdMarkoEdge);
                FillEdgeProperties(session, markoKnowsVadasEdge);

                Assert.AreEqual(markoKnowsVadasEdge.GetProperty("weight").Value.ToDouble(),
                                byIdMarkoEdge.GetProperty("weight").Value.ToDouble());
            }
        }

        [TestCase(CoreGraphTests.GremlinGroovy, "g.V().hasLabel('person').has('name', 'marko').as('a')" +
                                     ".outE('knows').as('b').inV().as('c', 'd')" +
                                     ".outE('created').as('e', 'f', 'g').inV().as('h').path()")]
        [TestCase(CoreGraphTests.BytecodeJson, "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[" +
                                     "[\"V\"],[\"has\",\"person\",\"name\",\"marko\"],[\"as\",\"a\"]," +
                                     "[\"outE\",\"knows\"],[\"as\",\"b\"],[\"inV\"],[\"as\",\"c\",\"d\"]," +
                                     "[\"outE\",\"created\"],[\"as\",\"e\",\"f\",\"g\"],[\"inV\"],[\"as\", \"h\"],[\"path\"]]}}")]
        public void Should_Retrieve_Path_With_Labels(string graphsonLanguage, string graphQuery)
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName).SetLanguage(graphsonLanguage))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement(graphQuery));

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

                    var marko = objects[0].To<IVertex>();
                    var knows = objects[1].To<IEdge>();
                    var josh = objects[2].To<IVertex>();
                    var created = objects[3].To<IEdge>();
                    var software = objects[4].To<IVertex>();

                    Assert.AreEqual("person", marko.Label);
                    Assert.AreEqual("person", josh.Label);
                    Assert.AreEqual("software", software.Label);

                    Assert.AreEqual("created", created.Label);
                    Assert.AreEqual("person", created.OutVLabel);
                    Assert.AreEqual("software", created.InVLabel);

                    Assert.AreEqual("knows", knows.Label);
                    Assert.AreEqual("person", knows.OutVLabel);
                    Assert.AreEqual("person", knows.InVLabel);

                    // DSE only with GraphSON1 provides properties by default
                    FillVertexProperties(session, marko);
                    FillVertexProperties(session, josh);
                    FillVertexProperties(session, software);
                    FillEdgeProperties(session, created);
                    FillEdgeProperties(session, knows);

                    Assert.AreEqual("marko", marko.GetProperty("name").Value.To<string>());
                    Assert.AreEqual(29, marko.GetProperty("age").Value.To<int>());
                    Assert.AreEqual("josh", josh.GetProperty("name").Value.To<string>());
                    Assert.AreEqual(32, josh.GetProperty("age").Value.To<int>());
                    Assert.AreEqual("java", software.GetProperty("lang").Value.To<string>());
                    if (software.GetProperty("name").Value.To<string>() == "lop")
                    {
                        Assert.AreEqual(0.4f, created.GetProperty("weight").Value.ToFloat());
                    }
                    else
                    {
                        Assert.AreEqual(1.0f, created.GetProperty("weight").Value.ToFloat());
                        Assert.AreEqual("ripple", software.GetProperty("name").Value.To<string>());
                    }
                    Assert.AreEqual(1f, knows.GetProperty("weight").Value.ToFloat());
                }
            }
        }

        [Test]
        public void Should_ThrowInvalidQueryException_When_VertexLabelDoesNotExist()
        {
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var ex = Assert.Throws<InvalidQueryException>(
                    () => session.ExecuteGraph(new SimpleGraphStatement("g.V().hasLabel('notALabel')")));

                Assert.IsTrue(ex.Message.Contains("Unknown vertex label 'notALabel'"), ex.Message);
            }
        }

        [Test]
        public void Should_Have_The_Same_ReadTimeout_Per_Statement_And_Global()
        {
            const int timeout = 2000;
            const int timeoutThreshold = timeout / 10; //10%
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName).SetReadTimeoutMillis(timeout))
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
            const int timeout = 5000;
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName).SetReadTimeoutMillis(timeout))
                .Build())
            {
                var session = cluster.Connect();
                var stopwatch = new Stopwatch();
                const int stmtTimeout = 1000;
                const int stmtTimeoutThreshold = stmtTimeout / 4; //25%
                try
                {
                    stopwatch.Start();
                    session.ExecuteGraph(new SimpleGraphStatement("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2500L);")
                                            .SetReadTimeoutMillis(stmtTimeout));
                }
                catch
                {
                    stopwatch.Stop();
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, stmtTimeout - stmtTimeoutThreshold);
                    Assert.Less(stopwatch.ElapsedMilliseconds, stmtTimeout + stmtTimeoutThreshold);
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
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName).SetReadTimeoutMillis(timeout))
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
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.V().hasLabel('person').has('name', 'marko').as('a').outE('knows').as('b').inV().as('c', 'd')" +
                    ".outE('created').as('e', 'f', 'g').inV().as('h').path()"));
                foreach (Path path in rs)
                {
                    CollectionAssert.AreEqual(
                        new[]
                        {
                            new [] { "a" }, new [] {"b"}, new[] { "c", "d" }, new[] { "e", "f", "g" }, new [] { "h" }
                        }, path.Labels);
                    var person = path.Objects.First().To<IVertex>();
                    Assert.AreEqual("person", person.Label);
                    FillVertexProperties(session, person);
                    Assert.NotNull(person.GetProperty("name"));
                }
            }
        }

        public static IEnumerable Should_Support_Types_TestCases
        {
            get
            {
                Func<object, string> durationToString = o => ((Duration)o).ToIsoString();
                Func<object, string> doubleToString = o => ((double)o).ToString(CultureInfo.InvariantCulture);
                Func<object, string> floatToString = o => ((float)o).ToString(CultureInfo.InvariantCulture);
                return new[]
                {
                    new TestCaseData("Boolean", true, "True", null, null),
                    new TestCaseData("Boolean", true, "True", null, null),
                    new TestCaseData("Boolean", false, "False", null, null),
                    new TestCaseData("Int", int.MaxValue, "2147483647", null, null),
                    new TestCaseData("Int", int.MinValue, "-2147483648", null, null),
                    new TestCaseData("Int", 0, "0", null, null),
                    new TestCaseData("Smallint", short.MaxValue, "32767", null, null),
                    new TestCaseData("Smallint", short.MinValue, "-32768", null, null),
                    new TestCaseData("Smallint", 0, "0", null, null),
                    new TestCaseData("Bigint", long.MaxValue, "9223372036854775807", null, null),
                    new TestCaseData("Bigint", long.MinValue, "-9223372036854775808", null, null),
                    new TestCaseData("Bigint", 0L, "0", null, null),
                    new TestCaseData("Float", 3.141592f, "3.141592", floatToString, null),
                    new TestCaseData("Double", 3.1415d, "3.1415", doubleToString, null),
                    new TestCaseData("Duration", Duration.Parse("P2DT3H4M"), "P2DT3H4M", durationToString, Duration.Parse("P2DT3H4M")),
                    new TestCaseData("Duration", Duration.Parse("PT5S"), "PT5S", durationToString, Duration.Parse("PT5S")),
                    new TestCaseData("Duration", Duration.Parse("PT1M"), "PT1M", durationToString, Duration.Parse("PT1M")),
                    new TestCaseData("Duration", Duration.Parse("PT1H1M"), "PT1H1M", durationToString, Duration.Parse("PT1H1M")),
                    new TestCaseData("Duration", Duration.Parse("PT240H"), "PT240H", durationToString, Duration.Parse("PT240H")),
                    new TestCaseData("Text", "The quick brown fox jumps over the lazy dog", "The quick brown fox jumps over the lazy dog", null, null)
                };
            }
        }

        [TestCaseSource(typeof(CoreGraphTests), nameof(CoreGraphTests.Should_Support_Types_TestCases))]
        public void Should_Support_Types
            (string type,
             object value,
             string expectedString,
             Func<object, string> toStringFunc,
             object expectedValue)
        {
            var id = _idGenerator++;
            var vertexLabel = "vertex" + id;
            var propertyName = "prop" + id;
            IncludeAndQueryVertex(vertexLabel, propertyName, type, value, expectedString, toStringFunc, true, expectedValue);
        }

        private IVertex IncludeAndQueryVertex(
            string vertexLabel, string propertyName, string type, object value,
            string expectedString, Func<object, string> toStringFunc, bool verifyToString = true,
            object expectedValue = null)
        {
            toStringFunc = toStringFunc ?? (o => o.ToString());
            IVertex vertex;
            using (var cluster = ClusterBuilder()
                .AddContactPoint(TestClusterManager.InitialContactPoint)
                .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                .Build())
            {
                var session = cluster.Connect();

                var schemaQuery = $"schema.vertexLabel(vertexLabel).partitionBy('pk', UUID).property(propertyName, {type}).create();";

                session.ExecuteGraph(new SimpleGraphStatement(schemaQuery, new { vertexLabel, propertyName }));

                var parameters = new { vertexLabel, propertyName, val = value, pk = Guid.NewGuid() };
                session.ExecuteGraph(new SimpleGraphStatement("g.addV(vertexLabel).property('pk', pk).property(propertyName, val)", parameters));

                var rs =
                    session.ExecuteGraph(
                        new SimpleGraphStatement("g.with('allow-filtering').V().hasLabel(vertexLabel).has('pk', pk).has(propertyName, val).next()", parameters));
                var first = rs.FirstOrDefault();
                Assert.NotNull(first);
                vertex = first.To<IVertex>();
                var expected = expectedValue ?? value;
                CoreGraphTests.ValidateVertexResult(
                    session, vertex, vertexLabel, propertyName, expectedString, toStringFunc, expected, expected.GetType(), verifyToString);
                FillVertexProperties(session, vertex);
            }
            return vertex;
        }

        private void TestInsertSelectProperty<T>(string type, T value, bool verifyToString = true)
        {
            var id = _idGenerator++;
            var vertexLabel = "vertex" + id;
            var propertyName = "prop" + id;
            var vertex = IncludeAndQueryVertex(vertexLabel, propertyName, type, value, value.ToString(), null, verifyToString);
            var propObject = vertex.GetProperty(propertyName).Value.To<T>();
            Assert.AreEqual(value, propObject);
        }

        [Test]
        public void Should_Support_Point()
        {
            var type = "Point";
            var point = new Point(0, 1);
            TestInsertSelectProperty(type, point);
        }

        [Test]
        public void Should_Support_Line()
        {
            var type = "LineString";
            var lineString = new LineString(new Point(0, 0), new Point(0, 1), new Point(1, 1));
            TestInsertSelectProperty(type, lineString);
        }

        [Test]
        public void Should_Support_Polygon()
        {
            var type = "Polygon";
            var polygon = new Polygon(new Point(-10, 10), new Point(10, 0), new Point(10, 10), new Point(-10, 10));
            TestInsertSelectProperty(type, polygon);
        }

        [Test]
        public void Should_Support_Inet()
        {
            var address = IPAddress.Parse("127.0.0.1");
            TestInsertSelectProperty("Inet", address);
        }

        [Test]
        public void Should_Support_Guid()
        {
            var guid = Guid.NewGuid();
            TestInsertSelectProperty("Uuid", guid);
        }

        [Test]
        public void Should_Support_Decimal()
        {
            var type = "Decimal";
            var decimalValue = 10.10M;
            TestInsertSelectProperty(type, decimalValue, false);
        }

        [Test]
        public void Should_Support_BigInteger()
        {
            var type = "Varint";
            var varint = BigInteger.Parse("8675309");
            TestInsertSelectProperty(type, varint);
        }

        [Test]
        public void Should_Support_Timestamp()
        {
            var type = "Timestamp";
            var timestamp = DateTimeOffset.Parse("2016-02-04T02:26:31.657Z");
            TestInsertSelectProperty(type, timestamp, false);
        }

        [Test]
        public void Should_Support_Blob()
        {
            var type = "Blob";
            var buf = new byte[] { 3, 5 };
            TestInsertSelectProperty(type, buf, false);
        }

        [Test, TestDseVersion(5, 1)]
        public void Should_Support_Date()
        {
            var type = "Date";
            var dates = new[]
            {
                LocalDate.Parse("1999-07-29"),
                new LocalDate(1960, 6, 12),
                new LocalDate(1981, 9, 14)
            };
            foreach (var date in dates)
            {
                TestInsertSelectProperty(type, date);
            }
        }

        [Test, TestDseVersion(5, 1)]
        public void Should_Support_Time()
        {
            var type = "Time";
            var times = new[]
            {
                LocalTime.Parse("00:00:01.001"),
                LocalTime.Parse("18:30:41.554")
            };
            foreach (var time in times)
            {
                TestInsertSelectProperty(type, time);
            }
        }

        [TestCase("1m1s")]
        [TestCase("30h")]
        [TestCase("30h20m")]
        [TestCase("20m")]
        [TestCase("56s")]
        [TestCase("567ms")]
        [TestCase("1950us")]
        [TestCase("1950µs")]
        [TestCase("1950000ns")]
        [TestCase("1950000NS")]
        [TestCase("-1950000ns")]
        public void Should_Support_Duration(string valueStr)
        {
            const string type = "Duration";
            TestInsertSelectProperty(type, Duration.Parse(valueStr), false);
        }

        [Test]
        public void Should_Support_BulkSet()
        {
            var expectedList = new[] { 29, 27, 32, 35 };
            var expectedMap = new Dictionary<int, int> { { 29, 1 }, { 27, 1 }, { 32, 1 }, { 35, 1 } };
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(TestClusterManager.InitialContactPoint)
                                 .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                 .Build())
            {
                var session = cluster.Connect();
                var rs = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.V().hasLabel('person').aggregate('x').by('age').cap('x')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var node = resultArray[0];

                CollectionAssert.AreEquivalent(expectedList, node.To<IEnumerable<int>>());
                CollectionAssert.AreEquivalent(expectedList, node.To<IList<int>>());
                CollectionAssert.AreEquivalent(expectedList, node.To<ICollection<int>>());
                CollectionAssert.AreEquivalent(expectedList, node.To<List<int>>());
                CollectionAssert.AreEquivalent(expectedList, node.To<IReadOnlyCollection<int>>());
                CollectionAssert.AreEquivalent(expectedList, node.To<IReadOnlyList<int>>());

                CollectionAssert.AreEquivalent(expectedMap, node.To<IDictionary<int, int>>());
                CollectionAssert.AreEquivalent(expectedMap, node.To<IReadOnlyDictionary<int, int>>());
                CollectionAssert.AreEquivalent(expectedMap, node.To<Dictionary<int, int>>());
            }
        }
        
        [Test]
        public void Should_Support_Udts()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(TestClusterManager.InitialContactPoint)
                                 .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                 .Build())
            {
                var session = cluster.Connect();
                session.UserDefinedTypes.Define(
                    UdtMap.For<Phone>(keyspace: CoreGraphTests.GraphName)
                          .Map(c => c.PhoneType, "phone_type"),
                    UdtMap.For<Contact>(keyspace: CoreGraphTests.GraphName)
                          .Map(c => c.FirstName, "first_name")
                          .Map(c => c.LastName, "last_name")
                          .Map(c => c.Birth, "birth_date")
                );
                var contacts = new List<Contact>
                {
                    new Contact
                    {
                        FirstName = "Walter", 
                        LastName = "White", 
                        Phones = new HashSet<Phone>
                        {
                            new Phone {Alias = "Wat2", Number = "2414817", PhoneType = PhoneType.Mobile},
                            new Phone {Alias = "Office", Number = "999"}
                        },
                        Emails = new List<string>()
                    }
                };
                session.ExecuteGraph(new SimpleGraphStatement(
                    "g.addV('users_contacts')" +
                    ".property('id', 305)" +
                    ".property('contacts', contacts)", new { contacts }));
                
                var rs = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.with('allow-filtering').V().hasLabel('users_contacts').has('id', 305).properties('contacts')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var queriedContacts = resultArray[0].To<IVertexProperty>().Value.To<List<Contact>>();
                Assert.AreEqual(contacts, queriedContacts);
                var queriedContactObjects = resultArray[0].To<IVertexProperty>().Value.To<List<dynamic>>().Select(o => (Contact)o);
                Assert.AreEqual(contacts, queriedContactObjects);
                var contactsDict = resultArray[0].To<IVertexProperty>().Value.To<List<IDictionary<string, GraphNode>>>();
                var expectedContact = contacts.Single();
                var dbC = contactsDict.Single();
                Assert.AreEqual(expectedContact.Emails, dbC["emails"].To<IEnumerable<string>>());
                CollectionAssert.AreEquivalent(expectedContact.Phones, dbC["phones"].To<IEnumerable<Phone>>());
                CollectionAssert.AreEquivalent(expectedContact.Phones, dbC["phones"].To<IEnumerable<GraphNode>>().Select(g => g.To<Phone>()));
                CollectionAssert.AreEquivalent(expectedContact.Phones.Select(p => p.Alias), dbC["phones"].To<IEnumerable<IDictionary<string, object>>>().Select(dict => (string) dict["alias"]));
            }
        }
        
        [Test]
        public void Should_Support_InnerUdt()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(TestClusterManager.InitialContactPoint)
                                 .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                 .Build())
            {
                var session = cluster.Connect();
                session.ExecuteGraph(new SimpleGraphStatement(
                    "schema.type('testudt').property('name', Text).property('nullable_int', Int).property('phone', frozen(typeOf('phone'))).create()"));
                session.UserDefinedTypes.Define(
                    UdtMap.For<TestUdt>(keyspace: CoreGraphTests.GraphName)
                          .Map(c => c.Name, "name")
                          .Map(c => c.NullableInt, "nullable_int")
                          .Map(c => c.Phone, "phone"),
                    UdtMap.For<Phone>(keyspace: CoreGraphTests.GraphName));
                var testudt = new TestUdt { NullableInt = 1, Name = "123", Phone = new Phone { Alias = "123" }};
                session.ExecuteGraph(new SimpleGraphStatement(
                    "schema.vertexLabel('test_udts').partitionBy('id', Int).property('test_udt', typeOf('testudt')).create()"));
                session.ExecuteGraph(new SimpleGraphStatement(
                    "g.addV('test_udts')" +
                    ".property('id', 305)" +
                    ".property('test_udt', testudtparam)", new { testudtparam = testudt }));
                
                var rs = session.ExecuteGraph(new SimpleGraphStatement(
                    "g.with('allow-filtering').V().hasLabel('test_udts').has('id', 305).properties('test_udt')"));
                var resultArray = rs.ToArray();
                Assert.AreEqual(1, resultArray.Length);
                var dbTestUdt = resultArray[0].To<IVertexProperty>().Value.To<TestUdt>();
                Assert.AreEqual(dbTestUdt.NullableInt, testudt.NullableInt);
                Assert.AreEqual(dbTestUdt.Name, testudt.Name);
                Assert.AreEqual(dbTestUdt.Phone, testudt.Phone);
            }
        }

        [Test]
        public void Should_Support_Collections()
        {
            using (var cluster = ClusterBuilder()
                                 .AddContactPoint(TestClusterManager.InitialContactPoint)
                                 .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                 .Build())
            {
                var session = cluster.Connect();
                var listName = "listProp";
                var setName = "setProp";
                var mapName = "mapProp";
                var vertexLabel = "vertexLabelCollections";

                session.ExecuteGraph(
                    new SimpleGraphStatement(
                        "schema.vertexLabel(vertexLabel)" +
                        ".partitionBy('uuid', UUID)" +
                        ".property(listName, listOf(listOf(Point)))" +
                        ".property(setName, listOf(setOf(Timestamp)))" +
                        ".property(mapName, mapOf(Inet, Polygon))" +
                        ".create()", new { vertexLabel, listName, setName, mapName }));

                var list = new List<Point>
                {
                    new Point(1, 2),
                    new Point(1.5, 3.5)
                };

                var array = new Point[]
                {
                    new Point(4, 5),
                    new Point(7.5, 6.5)
                };

                var listWithDuplicates = new Point[]
                {
                    new Point(4, 5),
                    new Point(7.5, 6.5),
                    new Point(7.5, 6.5),
                    new Point(4, 5)
                };

                var set = new HashSet<DateTimeOffset>
                {
                    new DateTimeOffset(12414124L.ToDateFromMillisecondsSinceEpoch().DateTime, TimeSpan.FromHours(2)),
                    new DateTimeOffset(55446464L.ToDateFromMillisecondsSinceEpoch().DateTime, TimeSpan.FromHours(-5)),
                    2141247L.ToDateFromMillisecondsSinceEpoch(),
                    834742874L.ToDateFromMillisecondsSinceEpoch()
                };

                var sortedSet = new SortedSet<DateTimeOffset>
                {
                    482174817L.ToDateFromMillisecondsSinceEpoch(),
                    981248124L.ToDateFromMillisecondsSinceEpoch(),
                    214261241424L.ToDateFromMillisecondsSinceEpoch()
                };

                var dateTimeSet = new HashSet<DateTime>
                {
                    new DateTime(2013, 2, 2, 2, 2, 2, DateTimeKind.Unspecified),
                    new DateTime(2015, 2, 2, 2, 2, 2, DateTimeKind.Local),
                    new DateTimeOffset(1232445L.ToDateFromMillisecondsSinceEpoch().DateTime, TimeSpan.FromHours(2)).DateTime,
                    new DateTimeOffset(13232124L.ToDateFromMillisecondsSinceEpoch().DateTime, TimeSpan.FromHours(-3)).DateTime,
                    new DateTimeOffset(124124124L.ToDateFromMillisecondsSinceEpoch().DateTime, TimeSpan.FromHours(-10)).UtcDateTime,
                    new DateTime(2014, 1, 1, 1, 1, 2, DateTimeKind.Utc)
                };

                var map = new Dictionary<IPAddress, Polygon>
                {
                    { IPAddress.Parse("127.0.0.1"), new Polygon(new Point(1, 3), new Point(3, -11.2), new Point(3, 6.2), new Point(1, 3)) },
                    { IPAddress.Parse("127.0.0.2"), new Polygon(new Point(2, 3), new Point(4, -11.2), new Point(4, 6.2), new Point(2, 3)) },
                    { IPAddress.Parse("127.0.0.3"), new Polygon(new Point(2, 4), new Point(4, -10.2), new Point(4, 7.2), new Point(2, 4)) }
                };

                var concurrentDictionary = new ConcurrentDictionary<IPAddress, Polygon>();
                concurrentDictionary.GetOrAdd(IPAddress.Parse("127.0.90.10"), _ => new Polygon(new Point(12, 13), new Point(14, -1.2), new Point(14, 16.2), new Point(12, 13)));
                concurrentDictionary.GetOrAdd(IPAddress.Parse("127.100.0.1"), _ => new Polygon(new Point(22, 13), new Point(24, -1.2), new Point(24, 16.2), new Point(22, 13)));
                concurrentDictionary.GetOrAdd(IPAddress.Parse("127.0.20.1"), _ => new Polygon(new Point(-12, 13), new Point(-14, -1.2), new Point(-14, 16.2), new Point(-12, 13)));

                var pk1 = Guid.NewGuid();
                var pk2 = Guid.NewGuid();
                var queryStr = "g.addV(vertexLabel)" +
                                    ".property('uuid', pk)" +
                                    ".property(listName, listValue)" +
                                    ".property(setName, setValue)" +
                                    ".property(mapName, mapValue)";

                session.ExecuteGraph(new SimpleGraphStatement(queryStr,
                    new
                    {
                        vertexLabel,
                        pk = pk1,
                        listName,
                        setName,
                        mapName,
                        listValue = new List<IEnumerable<Point>> { list, array, listWithDuplicates },
                        setValue = new List<ISet<DateTimeOffset>> { set, sortedSet, GraphTypes.AsSet(set.ToList()) },
                        mapValue = map
                    }));

                session.ExecuteGraph(new SimpleGraphStatement(queryStr,
                    new
                    {
                        vertexLabel,
                        pk = pk2,
                        listName,
                        setName,
                        mapName,
                        listValue = new List<List<Point>> { list },
                        setValue = new List<ISet<DateTime>> { dateTimeSet },
                        mapValue = concurrentDictionary
                    }));

                var rs = session.ExecuteGraph(
                    new SimpleGraphStatement("g.with('allow-filtering').V().hasLabel(vertexLabel)", new { vertexLabel }));

                var vertices = rs.To<IVertex>().ToList();
                foreach (var v in vertices)
                {
                    FillVertexProperties(session, v);
                }

                Assert.That(vertices.Select(v => v.GetProperty("uuid").Value.To<Guid>()), Is.EquivalentTo(new[] { pk1, pk2 }));

                rs = session.ExecuteGraph(
                    new SimpleGraphStatement("g.with('allow-filtering').V().hasLabel(vertexLabel).valueMap()", new { vertexLabel }));

                var properties = rs.To<IDictionary<string, IEnumerable<IGraphNode>>>().ToList();
                var vertexPropertiesByUuid = properties.ToDictionary(props => props["uuid"].Single().To<Guid>());

                void AssertVertex<TDateType>(
                    Guid pk,
                    IEnumerable<IEnumerable<Point>> expectedList,
                    IEnumerable<ISet<TDateType>> expectedSet,
                    IDictionary<IPAddress, Polygon> expectedMap)
                {
                    var vertex = vertexPropertiesByUuid[pk];

                    AssertNestedCollections(expectedList, vertex[listName].Single().To<List<IEnumerable<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<IEnumerable<IEnumerable<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<IEnumerable<List<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<List<List<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<IReadOnlyList<List<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<IReadOnlyList<IReadOnlyCollection<Point>>>());
                    AssertNestedCollections(expectedList, vertex[listName].Single().To<ICollection<ICollection<Point>>>());

                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<List<IEnumerable<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<IEnumerable<IEnumerable<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<IEnumerable<List<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<List<List<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<IReadOnlyList<List<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<IReadOnlyList<IReadOnlyCollection<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<ICollection<ICollection<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<IReadOnlyList<ISet<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<ICollection<HashSet<TDateType>>>());
                    AssertNestedCollections(expectedSet, vertex[setName].Single().To<ICollection<SortedSet<TDateType>>>());

                    CollectionAssert.AreEquivalent(expectedMap, vertex[mapName].Single().To<IDictionary<IPAddress, Polygon>>());
                    CollectionAssert.AreEquivalent(expectedMap, vertex[mapName].Single().To<Dictionary<IPAddress, Polygon>>());
                    CollectionAssert.AreEquivalent(expectedMap, vertex[mapName].Single().To<IReadOnlyDictionary<IPAddress, Polygon>>());
                }

                AssertVertex(
                    pk1,
                    new List<IEnumerable<Point>> { list, array, listWithDuplicates },
                    new List<ISet<DateTimeOffset>> { set, sortedSet, set },
                    map);

                AssertVertex(
                    pk2,
                    new List<List<Point>> { list },
                    new List<ISet<DateTime>> { dateTimeSet },
                    concurrentDictionary);
            }
        }

        private void AssertNestedCollections<T>(IEnumerable<IEnumerable<T>> expected, IEnumerable<IEnumerable<T>> actual)
        {
            var expectedList = expected.ToList();
            var actualList = actual.ToList();
            Assert.AreEqual(expectedList.Count, actualList.Count);
            for (var i = 0; i < expectedList.Count; i++)
            {
                CollectionAssert.AreEquivalent(
                    expectedList[i],
                    actualList[i]);
            }
        }

        [Test]
        public void ExecuteGraph_Should_SerializeAndDeserialize_OutOfRangeJavaDurations()
        {
            var values = new[]
            {
                new Duration(1, 0, 0),
                new Duration(-1, 0, 0)
            };
            using (var cluster = ClusterBuilder()
                                           .AddContactPoint(TestClusterManager.InitialContactPoint)
                                           .WithGraphOptions(new GraphOptions().SetName(CoreGraphTests.GraphName))
                                           .Build())
            {
                var session = cluster.Connect();
                session.ExecuteGraph(
                    new SimpleGraphStatement(
                        "schema.vertexLabel(vertexLabel)" +
                        ".partitionBy('uuid', UUID)" +
                        ".property(propertyName, Duration)" +
                        ".create()", new { vertexLabel = "v1", propertyName = "prop1" }));
                foreach (var value in values)
                {
                    var parameters = new { pk = Guid.NewGuid(), vertexLabel = "v1", propertyName = "prop1", val = value };
                    var stmt = new SimpleGraphStatement("g.addV(vertexLabel).property('uuid', pk).property(propertyName, val)", parameters);
                    session.ExecuteGraph(stmt);
                    var rs = session.ExecuteGraph(new SimpleGraphStatement("g.with('allow-filtering').V().hasLabel(vertexLabel).has('uuid', pk).has(propertyName, val).properties(propertyName).value()", parameters));
                    Assert.AreEqual(value, rs.Single().To<Duration>());
                }
            }
        }

        [Test]
        public async Task With_Bytecode_It_Should_Retrieve_Vertex_Instances()
        {
            var statement = new SimpleGraphStatement("{\"@type\": \"g:Bytecode\", \"@value\": {" +
                                                     "  \"step\": [[\"V\"], [\"hasLabel\", \"person\"]]}}");
            statement.SetGraphLanguage(CoreGraphTests.BytecodeJson);
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            var results = rs.To<IVertex>().ToArray();
            Assert.Greater(results.Length, 0);
            foreach (var vertex in results)
            {
                Assert.AreEqual("person", vertex.Label);
            }
        }

        [Test]
        public async Task With_Bytecode_It_Should_Retrieve_Edge_Instances()
        {
            var statement = new SimpleGraphStatement("{\"@type\": \"g:Bytecode\", \"@value\": {" +
                                                     "  \"step\": [[\"E\"], [\"hasLabel\", \"created\"]]}}");
            statement.SetGraphLanguage(CoreGraphTests.BytecodeJson);
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            var results = rs.To<IEdge>().ToArray();
            Assert.Greater(results.Length, 0);
            foreach (var edge in results)
            {
                Assert.AreEqual("created", edge.Label);
            }
        }

        [Test, TestDseVersion(5, 1)]
        public async Task With_Bytecode_It_Should_Insert_And_Retrieve_LocalDate_LocalTime()
        {
            const string schemaQuery = "schema.vertexLabel('typetests')" +
                                           ".partitionBy('name', Text)" +
                                           ".property('localdate', Date)" +
                                           ".property('localtime', Time)" +
                                           ".create();\n";

            _session.ExecuteGraph(new SimpleGraphStatement(schemaQuery));

            var deleteStatement = new SimpleGraphStatement("{\"@type\": \"g:Bytecode\", \"@value\": {" +
                                                           "  \"step\": [[\"V\"], " +
                                                           "    [\"has\", \"typetests\", \"name\", \"stephen\"]," +
                                                           "    [\"drop\"]]}}");
            deleteStatement.SetGraphLanguage(CoreGraphTests.BytecodeJson);
            _session.ExecuteGraph(deleteStatement);

            var addStatement = new SimpleGraphStatement("{\"@type\":\"g:Bytecode\", \"@value\": {\"step\":[" +
                                                        "[\"addV\", \"typetests\"],[\"property\",\"name\",\"stephen\"]," +
                                                        "[\"property\",\"localdate\", {\"@type\":\"gx:LocalDate\",\"@value\":\"1981-09-14\"}]," +
                                                        "[\"property\",\"localtime\", {\"@type\":\"gx:LocalTime\",\"@value\":\"12:50\"}]]}}");
            addStatement.SetGraphLanguage(CoreGraphTests.BytecodeJson);
            await _session.ExecuteGraphAsync(addStatement).ConfigureAwait(false);

            var statement = new SimpleGraphStatement("{\"@type\": \"g:Bytecode\", \"@value\": {" +
                                                     "  \"step\": [[\"V\"], [\"has\", \"typetests\", \"name\", \"stephen\"]]}}");
            statement.SetGraphLanguage(CoreGraphTests.BytecodeJson);
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            var results = rs.ToArray();
            Assert.AreEqual(1, results.Length);
            var stephen = results.First().To<IVertex>();
            FillVertexProperties(_session, stephen);
            Assert.AreEqual("stephen", stephen.GetProperty("name").Value.ToString());
            Assert.AreEqual(LocalDate.Parse("1981-09-14"), stephen.GetProperty("localdate").Value.To<LocalDate>());
            Assert.AreEqual(LocalTime.Parse("12:50"), stephen.GetProperty("localtime").Value.To<LocalTime>());
        }

        [TestCase(CoreGraphTests.BytecodeJson, "{\"@type\": \"g:Bytecode\", \"@value\": {\"step\": " +
                                     "[[\"V\"], [\"has\", \"person\", \"name\", \"marko\"], [\"outE\"]," +
                                     " [\"properties\"]]}}")]
        [TestCase(CoreGraphTests.GremlinGroovy, "g.V().has('person', 'name', 'marko').outE().properties()")]
        public void Should_Retrieve_Edge_Properties(string graphsonLanguage, string graphQuery)
        {
            var statement = new SimpleGraphStatement(graphQuery);
            statement.SetGraphLanguage(graphsonLanguage);
            var rs = _session.ExecuteGraph(statement);
            var results = rs.To<IProperty>().ToArray();
            Assert.Greater(results.Length, 1);
            Assert.True(results.Any(prop => prop.Name == "weight" && Math.Abs(prop.Value.To<double>() - 0.5) < 0.001));
        }

        [TestCase(CoreGraphTests.BytecodeJson, "{\"@type\": \"g:Bytecode\", \"@value\": {\"step\": " +
                                     "[[\"V\"], [\"has\", \"person\", \"name\", \"marko\"], [\"properties\"]]}}")]
        [TestCase(CoreGraphTests.GremlinGroovy, "g.V().has('person', 'name', 'marko').properties()")]
        public void Should_Retrieve_Vertex_Properties(string graphsonLanguage, string graphQuery)
        {
            var statement = new SimpleGraphStatement(graphQuery);
            statement.SetGraphLanguage(graphsonLanguage);
            var rs = _session.ExecuteGraph(statement);
            var results = rs.To<IVertexProperty>().ToArray();
            Assert.Greater(results.Length, 1);
            Assert.True(results.Any(prop => prop.Label == "name" && prop.Value.ToString() == "marko"));
        }

        [Test]
        public async Task With_Bytecode_It_Should_ParseMultipleRows()
        {
            var statement = new SimpleGraphStatement("{\"@type\": \"g:Bytecode\", \"@value\": {" +
                                                     "  \"step\": [[\"V\"], [\"hasLabel\", \"person\"]," +
                                                     "     [\"has\", \"name\", \"marko\"], [\"outE\"], [\"label\"]]}}")
                .SetGraphLanguage(CoreGraphTests.BytecodeJson);
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            Assert.That(rs.To<string>(), Is.EquivalentTo(new[] { "created", "knows", "knows" }));
            Assert.AreEqual(GraphProtocol.GraphSON3, rs.GraphProtocol);
        }

        [Test]
        public async Task With_GremlinGroovy_It_Should_ParseMultipleRows()
        {
            var statement = new SimpleGraphStatement("g.V().hasLabel('person').has('name', 'marko').outE().label()")
                .SetGraphLanguage(CoreGraphTests.GremlinGroovy);
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            Assert.That(rs.To<string>(), Is.EquivalentTo(new[] { "created", "knows", "knows" }));
            Assert.AreEqual(GraphProtocol.GraphSON3, rs.GraphProtocol);
        }

        [TestDseVersion(6, 8)]
        [Test]
        public async Task Should_UseGraphSON3_When_CoreGraphEngine()
        {
            var statement = new SimpleGraphStatement("g.V().hasLabel('person').has('name', 'marko').outE().label()");
            var rs = await _session.ExecuteGraphAsync(statement).ConfigureAwait(false);
            Assert.AreEqual(GraphProtocol.GraphSON3, rs.GraphProtocol);
        }

        [TestCase(GraphProtocol.GraphSON1, "Graph protocol 'GRAPHSON_1_0' incompatible")]
        [TestCase(GraphProtocol.GraphSON2, "Graph protocol 'GRAPHSON_2_0' incompatible")]
        [Test]
        public void Should_UseSpecifiedGraphProtocol_When_ItIsProvidedAtStatementLevel(GraphProtocol protocol, string msg)
        {
            var statement = new SimpleGraphStatement(
                "g.V().hasLabel('person').has('name', 'marko').outE().label()")
                .SetGraphProtocolVersion(protocol);
            var ex = Assert.Throws<InvalidQueryException>(() => _session.ExecuteGraph(statement));
            Assert.IsTrue(ex.Message.Contains(msg), ex.Message);
        }

        private static void ValidateVertexResult(
            ISession session, IVertex vertex, string vertexLabel, string propertyName,
            string expectedValueString, Func<object, string> toStringFunc, object value, Type type, bool verifyToString)
        {
            Assert.AreEqual(vertex.Label, vertexLabel);
            FillVertexProperties(session, vertex);
            Assert.AreEqual(value, vertex.GetProperty(propertyName).Value.To(type));
            if (verifyToString)
            {
                Assert.AreEqual(expectedValueString, toStringFunc(vertex.GetProperty(propertyName).Value.To(type)));
            }
        }

        private static void FillEdgeProperties(ISession session, IEdge edge)
        {
            var castedEdge = (Edge)edge;

            if (castedEdge.Properties.Count != 0)
            {
                return;
            }

            var rs = session.ExecuteGraph(
                new SimpleGraphStatement("g.E(edge_id).properties()", new { edge_id = castedEdge.Id })
                    .SetGraphLanguage("gremlin-groovy"));
            var propertiesList = rs.Select(p => new Tuple<GraphNode, IProperty>(p, p.To<IProperty>())).ToList();
            foreach (var prop in propertiesList)
            {
                castedEdge.Properties.Add(prop.Item2.Name, prop.Item1);
            }
        }

        private static void FillVertexProperties(ISession session, IVertex vertex)
        {
            var castedVertex = (Vertex)vertex;

            if (castedVertex.Properties.Count != 0)
            {
                return;
            }

            var rs = session.ExecuteGraph(
                new SimpleGraphStatement("g.V(vertex_id).properties().group().by(key)", new { vertex_id = castedVertex.Id })
                    .SetGraphLanguage("gremlin-groovy")).Single().To<IDictionary<string, GraphNode>>();

            foreach (var propertiesByName in rs)
            {
                // "properties" is a map where the key is the vertexproperty name
                // and the value is a json array of vertex properties with that name

                castedVertex.Properties.Add(
                    propertiesByName.Key,
                    propertiesByName.Value.Get<GraphNode>("@value"));
            }
        }

        private class TestUdt
        {
            public string Name { get; set; }

            public int? NullableInt { get; set; }

            public Phone Phone { get; set; }
        }
    }
}