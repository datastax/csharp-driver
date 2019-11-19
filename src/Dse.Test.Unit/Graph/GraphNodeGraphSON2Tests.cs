//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using Dse.Geometry;
using Dse.Graph;
using Dse.Serialization.Graph.GraphSON2;
using NUnit.Framework;

namespace Dse.Test.Unit.Graph
{
    public class GraphNodeGraphSON2Tests : BaseUnitTest
    {
        [TestCase("\"something\"", "something")]
        [TestCase("true", true)]
        [TestCase("false", false)]
        [TestCase("{\"@type\": \"g:Int16\", \"@value\": 12}", (short)12)]
        [TestCase("{\"@type\": \"g:Int32\", \"@value\": 123}", 123)]
        [TestCase("{\"@type\": \"g:Int64\", \"@value\": 456}", 456L)]
        [TestCase("{\"@type\": \"g:Float\", \"@value\": 123.1}", 123.1f)]
        [TestCase("{\"@type\": \"g:Double\", \"@value\": 456.12}", 456.12D)]
        public void To_Should_Parse_Scalar_Values<T>(string json, T value)
        {
            var node = GetGraphNode(json);
            Assert.AreEqual(node.To<T>(), value);
            Assert.True(node.IsScalar);
        }

        [Test]
        public void Implicit_Conversion_Operators_Test()
        {
            var intNode = GetGraphNode("{\"@type\": \"g:Int16\", \"@value\": 123}");
            Assert.AreEqual(123, (int) intNode);
            Assert.AreEqual(123L, (long) intNode);
            Assert.AreEqual((short)123, (short) intNode);
            Assert.AreEqual(123, (int) intNode);
            string stringValue = GetGraphNode("\"something\"");
            Assert.AreEqual("something", stringValue);
            bool boolValue = GetGraphNode("true");
            Assert.True(boolValue);
            var floatNode = GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 123.1}");
            Assert.AreEqual(123.1f, (float) floatNode);
            Assert.AreEqual(123.1D, (double) floatNode);
        }

        [Test]
        public void To_Should_Throw_For_Structs_With_Null()
        {
            var node = GetGraphNode("null");
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
        public void To_Should_Parse_Nullable_Values()
        {
            var nodes = new[] { GetGraphNode("null"), GetGraphNode("{\"@type\": \"gz:Obj\", \"@value\": null}") };
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
            Assert.AreEqual(1, GetGraphNode("{\"@type\": \"g:Int16\", \"@value\": 1}").To<short?>());
            Assert.AreEqual(1, GetGraphNode("{\"@type\": \"g:Int32\", \"@value\": 1}").To<int?>());
            Assert.AreEqual(1L, GetGraphNode("{\"@type\": \"g:Int64\", \"@value\": 1}").To<long?>());
            Assert.AreEqual(1M, GetGraphNode("{\"@type\": \"g:BigDecimal\", \"@value\": 1}").To<decimal?>());
            Assert.AreEqual(1F, GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 1}").To<float?>());
            Assert.AreEqual(1D, GetGraphNode("{\"@type\": \"g:Double\", \"@value\": 1}").To<double?>());
            Assert.AreEqual(Guid.Parse("2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a"), GetGraphNode(
                "{\"@type\": \"g:Int16\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}").To<Guid?>());
            Assert.AreEqual((TimeUuid) Guid.Parse("2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a"), GetGraphNode(
                "{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}").To<TimeUuid?>());
            Assert.AreEqual(BigInteger.Parse("1"), 
                            GetGraphNode("{\"@type\": \"g:Int16\", \"@value\": 1}").To<BigInteger?>());
            Assert.AreEqual(Duration.Parse("12h"), 
                            GetGraphNode("{\"@type\": \"g:Duration\", \"@value\": \"12h\"}").To<Duration?>());
            Assert.AreEqual(DateTimeOffset.Parse("1970-01-01 00:00:01Z"), 
                GetGraphNode("{\"@type\": \"gx:Instant\", \"@value\": 1000}").To<DateTimeOffset?>());
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
            var node = GetGraphNode(json);
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
            var node = GetGraphNode(json);
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
            var node = GetGraphNode(json);
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
            var node = GetGraphNode(json);
            Assert.True(node.HasProperty("prop1"));
            Assert.False(node.HasProperty("propZ"));
        }

        [Test]
        public void HasProperty_Should_Check_For_JSON_Objects()
        {
            const string json = "{\"prop1\": {\"@type\": \"g:Double\", \"@value\": 789}}";
            var node = GetGraphNode(json);
            Assert.True(node.HasProperty("prop1"));
            Assert.False(node.HasProperty("propZ"));
        }

        [Test]
        public void Dynamic_Should_Navigate_Properties_Of_GraphSON2_Objects()
        {
            dynamic node = GetGraphNode(
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
            TestToTypeParsing(json, stringValue, IPAddress.Parse);
        }

        [TestCase("{\"@type\": \"gx:Duration\", \"@value\": \"PT288H\"}", "PT288H")]
        public void To_Should_Parse_Duration_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, Duration.Parse);
        }

        [TestCase("{\"@type\": \"gx:BigDecimal\", \"@value\": \"123.12\"}", "123.12")]
        public void To_Should_Parse_Decimal_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, str => decimal.Parse(str, CultureInfo.InvariantCulture));
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"e86925f6-a066-4202-935b-a3e391223d91\"}", 
                  "e86925f6-a066-4202-935b-a3e391223d91")]
        public void To_Should_Parse_Uuid_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, Guid.Parse);
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}", 
            "2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a")]
        public void To_Should_Parse_TimeUuid_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, s => (TimeUuid)Guid.Parse(s));
        }

        [TestCase("{\"@type\": \"dse:Point\", \"@value\": \"POINT (1.1 2.1)\"}", "POINT (1.1 2.1)")]
        public void To_Should_Parse_Point_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, Point.Parse);
        }

        [TestCase("{\"@type\": \"dse:LineString\", \"@value\": \"LINESTRING (1 2, 2 3)\"}", "LINESTRING (1 2, 2 3)")]
        public void To_Should_Parse_LineString_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, LineString.Parse);
        }

        [TestCase("{\"@type\": \"dse:Polygon\", \"@value\": \"POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))\"}", 
            "POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))")]
        public void To_Should_Parse_Polygon_Values(string json, string stringValue)
        {
            TestToTypeParsing(json, stringValue, Polygon.Parse);
        }

        [TestCase("{\"@type\": \"dse:Blob\", \"@value\": \"V2hhdCdzIGhhcHBlbmluZyBQZXRlcg==\"}", 
            "What's happening Peter")]
        public void To_Should_Parse_Blob_Values(string json, string stringValue)
        {
            var node = GetGraphNode(json);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes(stringValue), node.To<byte[]>());
        }

        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": 1000}", 
            "1970-01-01 00:00:01Z")]
        public void To_Should_Parse_DateTimeOffset_Values(string json, string stringValue)
        {
            var node = GetGraphNode(json);
            Assert.AreEqual(DateTimeOffset.Parse(stringValue), node.To<DateTimeOffset>());
        }

        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"1981-09-14\"}", "1981-09-14")]
        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"-1\"}", "-1")]
        public void To_Should_Parse_LocalDate_Values(string json, string stringValue)
        {
            var node = GetGraphNode(json);
            Assert.AreEqual(LocalDate.Parse(stringValue), node.To<LocalDate>());
        }

        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"12:50\"}", "12:50")]
        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"6:50:07.12345678\"}", "6:50:07.12345678")]
        public void To_Should_Parse_LocalTime_Values(string json, string stringValue)
        {
            var node = GetGraphNode(json);
            Assert.AreEqual(LocalTime.Parse(stringValue), node.To<LocalTime>());
        }

        [Test]
        public void To_Should_Parse_Vertex_Values()
        {
            var node = GetGraphNode(
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
        public void To_Should_Parse_Null_Vertex_Edge_Or_Path()
        {
            var node = GetGraphNode("null");
            Assert.Null(node.To<Vertex>());
            Assert.Null(node.To<Edge>());
            Assert.Null(node.To<Path>());
            Assert.Null(node.To<IVertex>());
            Assert.Null(node.To<IEdge>());
            Assert.Null(node.To<IPath>());
        }

        /// <summary>
        /// Asserts that To{T}() method returns the expected instance.
        /// </summary>
        /// <param name="json">The node json</param>
        /// <param name="stringValue">The string representation of the expected value</param>
        /// <param name="parser">The parser used for the expected value</param>
        private static void TestToTypeParsing<T>(string json, string stringValue, Func<string, T> parser)
        {
            var node = GetGraphNode(json);
            Assert.AreEqual(node.To<T>(), parser(stringValue));
        }

        private static GraphNode GetGraphNode(string json)
        {
            return new GraphNode(new GraphSON2Node("{\"result\": " + json + "}"));
        }
    }
}
