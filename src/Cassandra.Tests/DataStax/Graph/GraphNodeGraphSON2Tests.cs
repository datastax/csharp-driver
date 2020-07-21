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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using Cassandra.DataStax.Graph;
using Cassandra.Geometry;
using Cassandra.Serialization.Graph.GraphSON2;
using Newtonsoft.Json;
using NUnit.Framework;
using Path = Cassandra.DataStax.Graph.Path;

namespace Cassandra.Tests.DataStax.Graph
{
    public class GraphNodeGraphSON2Tests : BaseUnitTest
    {
        [TestCase("\"something\"", "something")]
        [TestCase("true", true)]
        [TestCase("false", false)]
        [TestCase("{\"@type\": \"gx:Int16\", \"@value\": 12}", (short)12)]
        [TestCase("{\"@type\": \"g:Int32\", \"@value\": 123}", 123)]
        [TestCase("{\"@type\": \"g:Int64\", \"@value\": 456}", 456L)]
        [TestCase("{\"@type\": \"g:Float\", \"@value\": 123.1}", 123.1f)]
        [TestCase("{\"@type\": \"g:Double\", \"@value\": 456.12}", 456.12D)]
        public void To_Should_Parse_Scalar_Values<T>(string json, T value)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(node.To<T>(), value);
            Assert.True(node.IsScalar);
        }

        [Test]
        public void Implicit_Conversion_Operators_Test()
        {
            var intNode = GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 123}");
            Assert.AreEqual(123, (int)intNode);
            Assert.AreEqual(123L, (long)intNode);
            Assert.AreEqual((short)123, (short)intNode);
            string stringValue = GraphNodeGraphSON2Tests.GetGraphNode("\"something\"");
            Assert.AreEqual("something", stringValue);
            bool boolValue = GraphNodeGraphSON2Tests.GetGraphNode("true");
            Assert.True(boolValue);
            var floatNode = GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 123.1}");
            Assert.AreEqual(123.1f, (float) floatNode);
            Assert.AreEqual((double)123.1f, (double) floatNode);        }

        [Test]
        public void To_Should_Throw_For_Structs_With_Null()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode("null");
            Assert.Throws<InvalidOperationException>(() => node.To<short>());
            Assert.Throws<InvalidOperationException>(() => node.To<int>());
            Assert.Throws<InvalidOperationException>(() => node.To<long>());
            Assert.Throws<InvalidOperationException>(() => node.To<decimal>());
            Assert.Throws<InvalidOperationException>(() => node.To<float>());
            Assert.Throws<InvalidOperationException>(() => node.To<double>());
            Assert.Throws<InvalidOperationException>(() => node.To<Guid>());
            Assert.Throws<InvalidOperationException>(() => node.To<TimeUuid>());
            Assert.Throws<InvalidOperationException>(() => node.To<BigInteger>());
            Assert.Throws<InvalidOperationException>(() => node.To<Duration>());
        }
        [Test]
        public void To_Should_Not_Throw_For_Nullables_With_Null()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode("null");
            Assert.IsNull(node.To<short?>());
            Assert.IsNull(node.To<int?>());
            Assert.IsNull(node.To<long?>());
            Assert.IsNull(node.To<decimal?>());
            Assert.IsNull(node.To<float?>());
            Assert.IsNull(node.To<double?>());
            Assert.IsNull(node.To<Guid?>());
            Assert.IsNull(node.To<TimeUuid?>());
            Assert.IsNull(node.To<BigInteger?>());
            Assert.IsNull(node.To<Duration?>());
        }

        [Test]
        public void To_Should_Parse_Nullable_Values()
        {
            var nodes = new[] { GraphNodeGraphSON2Tests.GetGraphNode("null"), GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Vertex\", \"@value\": null}") };
            foreach (var node in nodes)
            {
                Assert.Null(node.To<short?>());
                Assert.Null(node.To<int?>());
                Assert.Null(node.To<long?>());
                Assert.Null(node.To<decimal?>());
                Assert.Null(node.To<float?>());
                Assert.Null(node.To<double?>());
                Assert.Null(node.To<Guid?>());
                Assert.Null(node.To<TimeUuid?>());
                Assert.Null(node.To<BigInteger?>());
                Assert.Null(node.To<Duration?>());
                Assert.Null(node.To<DateTimeOffset?>());
            }
            Assert.AreEqual(1, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 1}").To<short?>());
            Assert.AreEqual(1, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Int32\", \"@value\": 1}").To<int?>());
            Assert.AreEqual(1L, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Int64\", \"@value\": 1}").To<long?>());
            Assert.AreEqual(1M, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"gx:BigDecimal\", \"@value\": 1}").To<decimal?>());
            Assert.AreEqual(1F, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 1}").To<float?>());
            Assert.AreEqual(1D, GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Double\", \"@value\": 1}").To<double?>());
            Assert.AreEqual(null, GraphNodeGraphSON2Tests.GetGraphNode(
                "{\"@type\": \"gx:Int16\", \"@value\": null}").To<Guid?>());
            Assert.AreEqual((TimeUuid) Guid.Parse("2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a"), GraphNodeGraphSON2Tests.GetGraphNode(
                "{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}").To<TimeUuid?>());
            Assert.AreEqual(BigInteger.Parse("1"), 
                            GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 1}").To<BigInteger?>());
            Assert.AreEqual(Duration.Parse("12h"), 
                            GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"gx:Duration\", \"@value\": \"12h\"}").To<Duration?>());
            Assert.AreEqual(DateTimeOffset.Parse("1970-01-01 00:00:01Z"), 
                GraphNodeGraphSON2Tests.GetGraphNode("{\"@type\": \"g:Timestamp\", \"@value\": 1000}").To<DateTimeOffset?>());
        }

        [TestCase("\"something\"", "something")]
        [TestCase("true", true)]
        [TestCase("false", false)]
        [TestCase("{\"@type\": \"g:Int32\", \"@value\": 12356}", "12356")]
        [TestCase("{\"@type\": \"g:Int64\", \"@value\": 456}", "456")]
        [TestCase("{\"@type\": \"g:Float\", \"@value\": 123.1}", "123.1")]
        [TestCase("{\"@type\": \"g:Double\", \"@value\": 456.12}", "456.12")]
        public void ToString_Should_Return_The_String_Representation_Of_Scalars(string json, object value)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(node.ToString(), value.ToString());
        }

        [Test]
        public void Get_T_Should_Navigate_Properties_Of_GraphSON2_Objects()
        {
            const string json = "{\"@type\": \"gex:ObjectTree\", \"@value\": " +
                                "  {" +
                                "    \"prop1\": {\"@type\": \"g:Int32\", \"@value\": 789}," +
                                "    \"prop2\": \"prop2 value\"" +
                                "  }" +
                                "}";
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(789, node.Get<int>("prop1"));
            Assert.AreEqual("prop2 value", node.Get<string>("prop2"));
            var prop = node.Get<IGraphNode>("prop1");
            Assert.AreEqual(789, prop.To<int>());
            Assert.AreEqual("prop2 value", node.Get<IGraphNode>("prop2").ToString());
        }

        [Test]
        public void Get_T_Should_Navigate_Properties_Of_Json_Objects()
        {
            const string json = "{\"prop1\": {\"@type\": \"g:Double\", \"@value\": 789}," +
                                "  \"prop2\": true}";
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(789D, node.Get<double>("prop1"));
            Assert.AreEqual(true, node.Get<bool>("prop2"));
        }

        [Test]
        public void HasProperty_Should_Check_For_GraphSON2_Objects()
        {
            const string json = "{\"@type\": \"gex:ObjectTree\", \"@value\": " +
                                "  {" +
                                "    \"prop1\": {\"@type\": \"g:Int32\", \"@value\": 789}," +
                                "    \"prop2\": \"prop2 value\"" +
                                "  }" +
                                "}";
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.True(node.HasProperty("prop1"));
            Assert.False(node.HasProperty("propZ"));
        }

        [Test]
        public void HasProperty_Should_Check_For_JSON_Objects()
        {
            const string json = "{\"prop1\": {\"@type\": \"g:Double\", \"@value\": 789}}";
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.True(node.HasProperty("prop1"));
            Assert.False(node.HasProperty("propZ"));
        }

        [Test]
        public void Dynamic_Should_Navigate_Properties_Of_GraphSON2_Objects()
        {
            dynamic node = GraphNodeGraphSON2Tests.GetGraphNode(
                "{\"@type\": \"g:MyObject\", \"@value\": {" +
                "\"id\":{\"@type\":\"g:Int32\",\"@value\":150}," +
                "\"label\":\"topic\"" +
                "}}");
            int id = node.id;
            Assert.AreEqual(150, id);
            Assert.AreEqual("topic", node.label.ToString());
        }

        [TestCase("{\"@type\": \"gx:InetAddress\", \"@value\": \"127.0.0.1\"}", "127.0.0.1")]
        [TestCase("{\"@type\": \"gx:InetAddress\", \"@value\": \"ab::1\"}", "ab::1")]
        public void To_Should_Parse_InetAddress_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, IPAddress.Parse);
        }

        [TestCase("{\"@type\": \"gx:Duration\", \"@value\": \"PT288H\"}", "PT288H")]
        public void To_Should_Parse_Duration_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, Duration.Parse);
        }

        [TestCase("{\"@type\": \"gx:BigDecimal\", \"@value\": \"123.12\"}", "123.12")]
        public void To_Should_Parse_Decimal_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, str => decimal.Parse(str, CultureInfo.InvariantCulture));
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"e86925f6-a066-4202-935b-a3e391223d91\"}", 
                  "e86925f6-a066-4202-935b-a3e391223d91")]
        public void To_Should_Parse_Uuid_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, Guid.Parse);
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}", 
            "2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a")]
        public void To_Should_Parse_TimeUuid_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, s => (TimeUuid)Guid.Parse(s));
        }

        [TestCase("{\"@type\": \"dse:Point\", \"@value\": \"POINT (1.1 2.1)\"}", "POINT (1.1 2.1)")]
        public void To_Should_Parse_Point_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, Point.Parse);
        }

        [TestCase("{\"@type\": \"dse:LineString\", \"@value\": \"LINESTRING (1 2, 2 3)\"}", "LINESTRING (1 2, 2 3)")]
        public void To_Should_Parse_LineString_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, LineString.Parse);
        }

        [TestCase("{\"@type\": \"dse:Polygon\", \"@value\": \"POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))\"}", 
            "POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))")]
        public void To_Should_Parse_Polygon_Values(string json, string stringValue)
        {
            GraphNodeGraphSON2Tests.TestToTypeParsing(json, stringValue, Polygon.Parse);
        }

        [TestCase("{\"@type\": \"dse:Blob\", \"@value\": \"V2hhdCdzIGhhcHBlbmluZyBQZXRlcg==\"}", 
            "What's happening Peter")]
        public void To_Should_Parse_Blob_Values(string json, string stringValue)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes(stringValue), node.To<byte[]>());
        }

        [TestCase("{\"@type\": \"g:Timestamp\", \"@value\": 1000}", 
            "1970-01-01 00:00:01Z")]
        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}", 
            "2016-12-14T16:39:19.349Z")]
        [TestCase("null", null)]
        public void To_Should_Parse_DateTimeOffset_Values(string json, string stringValue)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            if (stringValue == null)
            {
                Assert.AreEqual(null, node.To<DateTimeOffset?>());
            }
            else
            {
                Assert.AreEqual(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), node.To<DateTimeOffset>());
                Assert.AreEqual(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), node.To<DateTimeOffset?>());
            }
        }
        
        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}", 
            "2016-12-14T16:39:19.349Z")]
        [TestCase("null", null)]
        public void To_Should_Parse_Instant_Values(string json, string stringValue)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            if (stringValue == null)
            {
                Assert.AreEqual(null, node.To<TinkerpopTimestamp?>());
            }
            else
            {
                Assert.AreEqual(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), node.To<TinkerpopTimestamp>());
                Assert.AreEqual(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), node.To<TinkerpopTimestamp?>());
                Assert.AreEqual(new TinkerpopTimestamp(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture)), node.To<TinkerpopTimestamp>());
            }
        }

        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"1981-09-14\"}", "1981-09-14")]
        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"-1\"}", "-1")]
        public void To_Should_Parse_LocalDate_Values(string json, string stringValue)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(LocalDate.Parse(stringValue), node.To<LocalDate>());
        }

        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"12:50\"}", "12:50")]
        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"6:50:07.12345678\"}", "6:50:07.12345678")]
        public void To_Should_Parse_LocalTime_Values(string json, string stringValue)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(LocalTime.Parse(stringValue), node.To<LocalTime>());
        }

        [Test]
        public void To_Should_Parse_Vertex_Values()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(
                "{\"@type\": \"g:Vertex\", \"@value\": {" +
                "\"id\":{\"@type\": \"g:Int32\",\"@value\": 1368843392}," +
                "\"label\":\"user\"," +
                "\"properties\":{" +
                "\"name\":[{\"@type\": \"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"jorge\"}}]," +
                "\"age\":[{\"@type\": \"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":35}}}]}" +
                "}}");
            var vertex = node.To<Vertex>();
            Assert.AreEqual("user", vertex.Label);
            Assert.AreEqual("jorge", vertex.Properties["name"].ToArray().First().Get<string>("value"));
            Assert.AreEqual(35, vertex.Properties["age"].ToArray().First().Get<int>("value"));
            var iVertex = node.To<IVertex>();
            Assert.AreEqual("user", iVertex.Label);
            Assert.AreEqual("jorge", iVertex.GetProperty("name").Value.ToString());
            Assert.AreEqual(35, iVertex.GetProperty("age").Value.To<int>());
            Assert.Null(iVertex.GetProperty("nonExistent"));
        }

        [Test]
        public void To_Should_Parse_Edge_Values()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(
                @"{
  ""@type"" : ""g:Edge"",
  ""@value"" : {
    ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 13 },
    ""label"" : ""develops"",
    ""inVLabel"" : ""software"",
    ""outVLabel"" : ""person"",
    ""inV"" : { ""@type"" : ""g:Int32"",""@value"" : 10 },
    ""outV"" : {""@type"" : ""g:Int32"",""@value"" : 1 },
    ""properties"" : { ""since"" : { ""@type"" : ""g:Int32"", ""@value"" : 2009 } }
  }
}");
            var edge = node.To<Edge>();
            Assert.AreEqual("develops", edge.Label);
            Assert.AreEqual("software", edge.InVLabel);
            Assert.AreEqual("person", edge.OutVLabel);
            Assert.AreEqual(10, edge.InV.To<int>());
            Assert.AreEqual(1, edge.OutV.To<int>());
            Assert.AreEqual(2009, edge.Properties["since"].To<int>());
            Assert.AreEqual(2009, edge.GetProperty("since").Value.To<int>());
            Assert.Null(edge.GetProperty("nonExistent"));
        }

        [Test]
        public void To_Should_Parse_Path_Values()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(
                @"{
  ""@type"" : ""g:Path"",
  ""@value"" : {
    ""labels"" : [ [ ], [ ], [ ] ],
    ""objects"" : [ {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 1 },
        ""label"" : ""person""
      }
    }, {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 10 },
        ""label"" : ""software"",
        ""properties"" : { ""name"" : [ { ""@type"" : ""g:VertexProperty"", ""@value"" : { ""id"" : { ""@type"" : ""g:Int64"", ""@value"" : 4 }, ""value"" : ""gremlin"", ""vertex"" : { ""@type"" : ""g:Int32"", ""@value"" : 10 }, ""label"" : ""name"" } } ]
     }
      }
    }, {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 11 },
        ""label"" : ""software"",
        ""properties"" : {
          ""name"" : [ 
		      { ""@type"" : ""g:VertexProperty"", ""@value"" : { ""id"" : { ""@type"" : ""g:Int64"", ""@value"" : 5 }, ""value"" : ""tinkergraph"", ""vertex"" : { ""@type"" : ""g:Int32"", ""@value"" : 11 }, ""label"" : ""name"" } } ]
        }
      }
    } ]
  }
}");
            var path = node.To<Path>();
            Assert.AreEqual(3, path.Labels.Count);
            Assert.IsTrue(path.Labels.All(c => c.Count == 0));

            var firstVertex = path.Objects.ElementAt(0).To<Vertex>();
            Assert.AreEqual(1, firstVertex.Id.To<int>());
            Assert.AreEqual("person", firstVertex.Label);
            Assert.AreEqual(0, firstVertex.Properties.Count);
            
            var secondVertex = path.Objects.ElementAt(1).To<Vertex>();
            Assert.AreEqual(10, secondVertex.Id.To<int>());
            Assert.AreEqual("software", secondVertex.Label);
            Assert.AreEqual(1, secondVertex.Properties.Count);
            Assert.AreEqual(1, secondVertex.Properties.Single().Value.To<IEnumerable<VertexProperty>>().Count());
            var secondVertexProperty = secondVertex.GetProperty("name");
            Assert.AreEqual(4L, secondVertexProperty.Id.To<long>());
            Assert.AreEqual("gremlin", secondVertexProperty.Value.To<string>());
            Assert.AreEqual("name", secondVertexProperty.Label);
            Assert.AreEqual(10, secondVertexProperty.Vertex.To<int>());
            
            var thirdVertex = path.Objects.ElementAt(2).To<Vertex>();
            Assert.AreEqual(11, thirdVertex.Id.To<int>());
            Assert.AreEqual("software", thirdVertex.Label);
            Assert.AreEqual(1, thirdVertex.Properties.Count);
            Assert.AreEqual(1, thirdVertex.Properties.Single().Value.To<IEnumerable<VertexProperty>>().Count());
            var thirdVertexProperty = thirdVertex.GetProperty("name");
            Assert.AreEqual(5L, thirdVertexProperty.Id.To<long>());
            Assert.AreEqual("tinkergraph", thirdVertexProperty.Value.To<string>());
            Assert.AreEqual("name", thirdVertexProperty.Label);
            Assert.AreEqual(11, thirdVertexProperty.Vertex.To<int>());
        }

        [Test]
        public void To_Should_Parse_Null_Vertex_Edge_Or_Path()
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode("null");
            Assert.Null(node.To<Vertex>());
            Assert.Null(node.To<Edge>());
            Assert.Null(node.To<Path>());
            Assert.Null(node.To<IVertex>());
            Assert.Null(node.To<IEdge>());
            Assert.Null(node.To<IPath>());
        }
        
        [Test]
        public void GraphNode_Ctor_Should_ThrowNotSupported_When_TypedGraphSON()
        {
            const string json = "{\"@type\":\"g:Vertex\",\"@value\":{" +
                                "\"id\":{\"@type\":\"g:Int32\",\"@value\":1368843392}," +
                                "\"label\":\"user\"," +
                                "\"properties\":{" +
                                "\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"jorge\"}}]," +
                                "\"age\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":35}}}]}" +
                                "}}";
            Assert.Throws<NotSupportedException>(() => new GraphNode("{\"result\":" + json + "}"));
        }

        /// <summary>
        /// Asserts that To{T}() method returns the expected instance.
        /// </summary>
        /// <param name="json">The node json</param>
        /// <param name="stringValue">The string representation of the expected value</param>
        /// <param name="parser">The parser used for the expected value</param>
        private static void TestToTypeParsing<T>(string json, string stringValue, Func<string, T> parser)
        {
            var node = GraphNodeGraphSON2Tests.GetGraphNode(json);
            Assert.AreEqual(node.To<T>(), parser(stringValue));
        }

        private static GraphNode GetGraphNode(string json)
        {
            return new GraphNode(new GraphSONNode("{\"result\": " + json + "}"));
        }
    }
}
