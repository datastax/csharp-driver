//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Dse.Geometry;
using Dse.Graph;
using Dse.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Dse.Test.Unit.Graph
{
    public class GraphNodeGraphSON1Tests : BaseUnitTest
    {
        [Test]
        public void Constructor_Should_Throw_When_Json_Is_Null()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new GraphNode((string)null));
        }

        [Test]
        public void Constructor_Should_Parse_Json()
        {
            var result = new GraphNode("{\"result\": \"something\"}");
            Assert.AreEqual("something", result.ToString());
            Assert.False(result.IsObjectTree);
            Assert.True(result.IsScalar);
            Assert.False(result.IsArray);

            result = new GraphNode("{\"result\": {\"something\": 1.2 }}");
            Assert.AreEqual(1.2D, result.Get<double>("something"));
            Assert.True(result.IsObjectTree);
            Assert.False(result.IsScalar);
            Assert.False(result.IsArray);

            result = new GraphNode("{\"result\": [] }");
            Assert.False(result.IsObjectTree);
            Assert.False(result.IsScalar);
            Assert.True(result.IsArray);
        }

        [Test]
        public void Should_Throw_For_Trying_To_Access_Properties_When_The_Node_Is_Not_An_Object_Tree()
        {
            var result = new GraphNode("{\"result\": {\"something\": 1.2 }}");
            Assert.True(result.IsObjectTree);
            Assert.True(result.HasProperty("something"));
            Assert.False(result.HasProperty("other"));

            //result is a scalar value
            result = new GraphNode("{\"result\": 1.2}");
            Assert.True(result.IsScalar);
            Assert.False(result.HasProperty("whatever"));
            Assert.Throws<InvalidOperationException>(() => result.GetProperties());
        }

        [Test]
        public void Get_T_Should_Allow_Serializable_Types()
        {
            TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2M);
            TestGet("{\"result\": {\"something\": 12 }}", "something", 12);
            TestGet("{\"result\": {\"something\": 12 }}", "something", 12L);
            TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2D);
            TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2F);
            TestGet("{\"result\": {\"something\": 1.2 }}", "something", "1.2");
            TestGet("{\"result\": {\"something\": \"123e4567-e89b-12d3-a456-426655440000\" }}", "something",
                Guid.Parse("123e4567-e89b-12d3-a456-426655440000"));
            TestGet("{\"result\": {\"something\": 12 }}", "something", BigInteger.Parse("12"));
            TestGet("{\"result\": {\"something\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\" }}", "something",
                (TimeUuid)Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            TestGet("{\"result\": {\"something\": [1, 2, 3] }}", "something", new[] { 1, 2, 3 });
            TestGet<IEnumerable<int>>("{\"result\": {\"something\": [1, 2, 3] }}", "something", new[] { 1, 2, 3 });
        }

        [Test]
        public void Get_T_Should_Allow_Geometry_Types()
        {
            TestGet("{\"result\": {\"something\": \"POINT (1.0 2.0)\" }}", "something", new Point(1, 2));
            TestGet("{\"result\": {\"something\": \"LINESTRING (1 2, 3 4.1234)\" }}", "something",
                new LineString(new Point(1, 2), new Point(3, 4.1234)));
            TestGet("{\"result\": {\"something\": \"POLYGON ((1 3, 3 1, 3 6, 1 3))\" }}", "something",
                new Polygon(new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)));
        }

        [Test]
        public void To_T_Should_Allow_Serializable_Types()
        {
            TestTo("{\"result\": 2.2}", 2.2M);
            TestTo("{\"result\": 2.2}", 2.2D);
            TestTo("{\"result\": 2.2}", 2.2F);
            TestTo("{\"result\": 22}", 22);
            TestTo("{\"result\": 22}", (int?)22);
            TestTo("{\"result\": 22}", 22L);
            TestTo("{\"result\": 22}", BigInteger.Parse("22"));
            TestTo("{\"result\": 22}", "22");
            TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", (Guid?) Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", (TimeUuid)Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
        }

        [Test]
        public void To_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": \"123\"}";
            var types = new [] { typeof(UIntPtr), typeof(IntPtr), typeof(StringBuilder) };
            foreach (var t in types)
            {
                Assert.Throws<NotSupportedException>(() => new GraphNode(json).To(t));
            }
        }

        [Test]
        public void To_T_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": \"123\"}";
            TestToThrows<IntPtr, NotSupportedException>(json);
            TestToThrows<UIntPtr, NotSupportedException>(json);
            TestToThrows<StringBuilder, NotSupportedException>(json);
        }

        [Test]
        public void Get_T_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": {\"something\": \"123\" }}";
            TestGetThrows<IntPtr, NotSupportedException>(json, "something");
            TestGetThrows<UIntPtr, NotSupportedException>(json, "something");
            TestGetThrows<StringBuilder, NotSupportedException>(json, "something");
        }

        private static void TestGet<T>(string json, string property, T expectedValue)
        {
            var result = new GraphNode(json);
            if (expectedValue is IEnumerable)
            {
                CollectionAssert.AreEqual((IEnumerable)expectedValue, (IEnumerable)result.Get<T>(property));
                return;
            }
            Assert.AreEqual(expectedValue, result.Get<T>(property));
        }

        private static void TestGetThrows<T, TException>(string json, string property) where TException : Exception
        {
            Assert.Throws<TException>(() => new GraphNode(json).Get<T>(property));
        }

        private static void TestTo<T>(string json, T expectedValue)
        {
            var result = new GraphNode(json);
            Assert.AreEqual(expectedValue, result.To<T>());
        }

        private static void TestToThrows<T, TException>(string json) where TException : Exception
        {
            Assert.Throws<TException>(() => new GraphNode(json).To<T>());
        }

        [Test]
        public void Should_Allow_Nested_Properties_For_Object_Trees()
        {
            dynamic result = new GraphNode("{\"result\": " +
                                             "{" +
                                                "\"something\": {\"inTheAir\": 1}," +
                                                "\"everything\": {\"isAwesome\": [1, 2, \"zeta\"]}, " +
                                                "\"a\": {\"b\": {\"c\": 0.6}} " +
                                             "}}");
            Assert.AreEqual(1, result.something.inTheAir);
            IEnumerable<GraphNode> values = result.everything.isAwesome;
            CollectionAssert.AreEqual(new [] { "1", "2", "zeta" }, values.Select(x => x.ToString()));
            Assert.AreEqual(0.6D, result.a.b.c);
        }

        [Test]
        public void ToString_Should_Return_The_Json_Representation_Of_Result_Property()
        {
            var result = new GraphNode("{\"result\": 1.9}");
            Assert.AreEqual("1.9", result.ToString());
            result = new GraphNode("{\"result\": [ 1, 2]}");
            Assert.AreEqual(string.Format("[{0}  1,{0}  2{0}]", Environment.NewLine), result.ToString());
            result = new GraphNode("{\"result\": \"a\"}");
            Assert.AreEqual("a", result.ToString());
        }

        [Test]
        public void ToDouble_Should_Convert_To_Double()
        {
            var result = new GraphNode("{\"result\": 1.9}");
            Assert.AreEqual(1.9, result.ToDouble());
        }

        [Test]
        public void ToDouble_Should_Throw_For_Non_Scalar_Values()
        {
            var result = new GraphNode("{\"result\": {\"something\": 0 }}");
            Assert.Throws<InvalidOperationException>(() => result.ToDouble());
        }

        [Test]
        public void Get_T_Should_Get_A_Typed_Value_By_Name()
        {
            var result = new GraphNode("{\"result\": {\"some\": \"value1\" }}");
            Assert.AreEqual("value1", result.Get<string>("some"));
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"something\": {\"is_awesome\": true} }}");
            Assert.AreEqual(true, result.Get<dynamic>("something").is_awesome);
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Nested_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"everything\": {\"is_awesome\": {\"when\": {" +
                                       "    \"we\": \"are together\"} }} }}");
            var everything = result.Get<dynamic>("everything");
            Assert.AreEqual("are together", everything.is_awesome.when.we);
        }

        [Test]
        public void Get_T_Should_Allow_GraphNode_For_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"something\": {\"is_awesome\": {\"it\": \"maybe\" }} }}");
            var node = result.Get<GraphNode>("something");
            Assert.NotNull(node);
            Assert.NotNull(node.Get<GraphNode>("is_awesome"));
            Assert.AreEqual("maybe", node.Get<GraphNode>("is_awesome").Get<string>("it"));
        }

        [Test]
        public void Get_T_Should_Not_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            var result = new GraphNode("{\"result\": {\"everything\": {\"is_awesome\": true} }}");
            Assert.DoesNotThrow(() => result.Get<dynamic>("what"));
        }

        [Test]
        public void Equals_Should_Return_True_For_The_Same_Json()
        {
            var result1 = new GraphNode("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result2 = new GraphNode("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result3 = new GraphNode("{\"result\": {\"other\": \"value\"}}");
            Assert.True(result1.Equals(result2));
            Assert.True(result2.Equals(result1));
            Assert.False(result1.Equals(result3));
            //operator
            Assert.True(result1 == result2);
            Assert.AreEqual(result1.GetHashCode(), result1.GetHashCode());
            Assert.AreEqual(result1.GetHashCode(), result2.GetHashCode());
            Assert.AreNotEqual(result1.GetHashCode(), result3.GetHashCode());
        }

        [Test]
        public void ToVertex_Should_Convert_To_Vertex()
        {
            var result = new GraphNode("{" +
              "\"result\": {" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"label\":\"vertex\"," +
                "\"type\":\"vertex\"," +
                "\"properties\":{" +
                  "\"name\":[{\"id\":{\"local_id\":\"00000000-0000-8007-0000-000000000000\",\"~type\":\"name\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":\"j\",\"label\":\"name\"}]," +
                  "\"age\":[{\"id\":{\"local_id\":\"00000000-0000-8008-0000-000000000000\",\"~type\":\"age\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":34,\"label\":\"age\"}]}" +
               "}}");
            var vertex = result.ToVertex();
            Assert.AreEqual("vertex", vertex.Label);
            dynamic id = vertex.Id;
            Assert.AreEqual(586910, id.community_id);
            Assert.AreEqual(586910, vertex.Id.Get<long>("community_id"));
            Assert.AreEqual(2, vertex.Properties.Count);
            dynamic nameProp = vertex.Properties["name"].ToArray();
            Assert.NotNull(nameProp);
            Assert.NotNull(nameProp[0].id);
            
            // Validate properties
            var properties = vertex.GetProperties();
            CollectionAssert.AreEquivalent(new[] {"name", "age"}, properties.Select(p => p.Name));
            var nameProperty = vertex.GetProperty("name");
            Assert.NotNull(nameProperty);
            Assert.AreEqual("j", nameProperty.Value.ToString());
            Assert.AreEqual(0, nameProperty.GetProperties().Count());
            var ageProperty = vertex.GetProperty("age");
            Assert.NotNull(ageProperty);
            Assert.AreEqual(34, ageProperty.Value.To<int>());
            Assert.AreEqual(0, ageProperty.GetProperties().Count());
            
            //Is convertible
            Assert.NotNull((Vertex)result);
            //Any enumeration of graph result can be casted to vertex
            IEnumerable<GraphNode> results = new[] { result, result, result };
            foreach (Vertex v in results)
            {
                Assert.NotNull(v);
            }
        }

        [Test, TestCase(true)]
#if NET452
        [TestCase(false)]
#endif
        public void GraphNode_Should_Be_Serializable(bool useConverter)
        {
            var settings = new JsonSerializerSettings();
            if (useConverter)
            {
                settings = GraphSON1ContractResolver.Settings;
            }
            const string json = "{" +
                "\"~type\":\"knows\"," +
                "\"out_vertex\":{\"~label\":\"person\",\"community_id\":1368843392,\"member_id\":2}," +
                "\"in_vertex\":{\"~label\":\"person\",\"community_id\":1368843392,\"member_id\":3}," +
                "\"local_id\":\"ed37c460-b2f7-11e6-b394-2d62a0c6b98b\"}";
            var node = new GraphNode("{\"result\":" + json + "}");
            var serialized = JsonConvert.SerializeObject(node, settings);
            Assert.AreEqual(json, serialized);
        }

        [Test]
        public void ToVertex_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphNode("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToVertex());
        }

        [Test]
        public void ToVertex_Should_Not_Throw_When_The_Properties_Is_Not_Present()
        {
            var vertex = GetGraphNode(
                "{" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"label\":\"vertex1\"," +
                "\"type\":\"vertex\"" +
                "}").ToVertex();
            Assert.AreEqual("vertex1", vertex.Label);
            Assert.NotNull(vertex.Id);
        }

        [Test]
        public void ToVertex_Should_Throw_When_Required_Attributes_Are_Not_Present()
        {
            Assert.Throws<InvalidOperationException>(() => GetGraphNode(
                "{" +
                "\"label\":\"vertex1\"," +
                "\"type\":\"vertex\"" +
                "}").ToVertex());
            Assert.Throws<InvalidOperationException>(() => GetGraphNode(
                "{" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"type\":\"vertex\"" +
                "}").ToVertex());
        }

        [Test]
        public void ToEdge_Should_Convert()
        {
            var result = new GraphNode("{" +
              "\"result\":{" +
                "\"id\":{" +
                    "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                    "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"," +
                "\"outVLabel\":\"vertex\"," +
                "\"inV\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5}," +
                "\"outV\":{\"member_id\":0,\"community_id\":680140,\"~label\":\"vertex\",\"group_id\":3}," +
                "\"properties\":{\"weight\":1.5}" +
                "}}");
            var edge = result.ToEdge();
            Assert.AreEqual("knows", edge.Label);
            Assert.AreEqual("in-vertex", edge.InVLabel);
            dynamic id = edge.Id;
            Assert.AreEqual("4e78f871-c5c8-11e5-a449-130aecf8e504", id.local_id);
            Assert.AreEqual(680140, edge.OutV.Get<long>("community_id"));
            Assert.AreEqual(1, edge.Properties.Count);
            var weightProp = edge.Properties["weight"];
            Assert.NotNull(weightProp);
            Assert.AreEqual(1.5D, weightProp.ToDouble());
            var property = edge.GetProperty("weight");
            Assert.NotNull(property);
            Assert.AreEqual("weight", property.Name);
            Assert.AreEqual(1.5D, property.Value.To<double>());
            
            Assert.Null(edge.GetProperty("nonExistentProperty"));
            
            //Is convertible
            Assert.NotNull((Edge)result);
            //Any enumeration of graph result can be casted to edge
            IEnumerable<GraphNode> results = new[] { result, result, result };
            foreach (Edge v in results)
            {
                Assert.NotNull(v);
            }
        }

        [Test]
        public void ToEdge_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphNode("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToEdge());
        }

        [Test]
        public void ToEdge_Should_Not_Throw_When_The_Properties_Is_Not_Present()
        {
            var edge = GetGraphNode("{" +
                "\"id\":{" +
                    "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                    "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge();
            Assert.AreEqual("knows", edge.Label);
            Assert.AreEqual("in-vertex", edge.InVLabel);
            Assert.Null(edge.OutVLabel);
        }


        [Test]
        public void ToEdge_Should_Throw_When_Required_Attributes_Are_Not_Present()
        {
            Assert.Throws<InvalidOperationException>(() => GetGraphNode(
                "{" +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge());
            
            Assert.Throws<InvalidOperationException>(() => GetGraphNode(
                "{" +
                "\"id\":{" +
                "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge());
        }

        [Test]
        public void ToPath_Should_Convert()
        {
            const string pathJson = "{\"result\":" + 
                "{" +
                "  \"labels\": [" +
                "    [\"a\"]," +
                "    []," +
                "    [\"c\", \"d\"]," +
                "    [\"e\", \"f\", \"g\"]," +
                "    []" +
                "  ]," +
                "  \"objects\": [" +
                "    {" +
                "      \"id\": {" +
                "        \"member_id\": 0,                                                        " +
                "        \"community_id\": 214210,                                                " +
                "        \"~label\": \"person\",                                                  " +
                "        \"group_id\": 3                                                          " +
                "      }," +
                "      \"label\": \"person\",                                                     " +
                "      \"type\": \"vertex\",                                                      " +
                "      \"properties\": {                                                          " +
                "        \"name\": [" +
                "          {" +
                "            \"id\": {                                                            " +
                "              \"local_id\": \"00000000-0000-7fff-0000-000000000000\",            " +
                "              \"~type\": \"name\",                                               " +
                "              \"out_vertex\": {                                                  " +
                "                \"member_id\": 0,                                                " +
                "                \"community_id\": 214210,                                        " +
                "                \"~label\": \"person\",                                          " +
                "                \"group_id\": 3                                                  " +
                "              }                                                                  " +
                "            },                                                                   " +
                "            \"value\": \"marko\"                                                 " +
                "          }" +
                "        ]," +
                "        \"age\": [                                                               " +
                "          {                                                                      " +
                "            \"id\": {                                                            " +
                "              \"local_id\": \"00000000-0000-8000-0000-000000000000\",            " +
                "              \"~type\": \"age\",                                                " +
                "              \"out_vertex\": {                                                  " +
                "                \"member_id\": 0,                                                " +
                "                \"community_id\": 214210,                                        " +
                "                \"~label\": \"person\",                                          " +
                "                \"group_id\": 3                                                  " +
                "              }                                                                  " +
                "            },                                                                   " +
                "            \"value\": 29                                                        " +
                "          }" +
                "        ]" +
                "      }" +
                "    }," +
                "    {" +
                "      \"id\": {" +
                "        \"out_vertex\": {" +
                "          \"member_id\": 0,                                                      " +
                "          \"community_id\": 214210,                                              " +
                "          \"~label\": \"person\",                                                " +
                "          \"group_id\": 3                                                        " +
                "        },                                                                       " +
                "        \"local_id\": \"77cd1b50-ffcc-11e5-aa66-231205ad38c3\",                  " +
                "        \"in_vertex\": {" +
                "          \"member_id\": 0,                                                      " +
                "          \"community_id\": 214210,                                              " +
                "          \"~label\": \"person\",                                                " +
                "          \"group_id\": 5                                                        " +
                "        },                                                                       " +
                "        \"~type\": \"knows\"                                                     " +
                "      }," +
                "      \"label\": \"knows\",                                                      " +
                "      \"type\": \"edge\",                                                        " +
                "      \"inVLabel\": \"person\",                                                  " +
                "      \"outVLabel\": \"person\",                                                 " +
                "      \"inV\": {" +
                "        \"member_id\": 0," +
                "        \"community_id\": 214210," +
                "        \"~label\": \"person\"," +
                "        \"group_id\": 5" +
                "      }," +
                "      \"outV\": {" +
                "        \"member_id\": 0," +
                "        \"community_id\": 214210," +
                "        \"~label\": \"person\"," +
                "        \"group_id\": 3" +
                "      }," +
                "      \"properties\": {" +
                "        \"weight\": 1.0" +
                "      }" +
                "    }" +
                "  ]" +
                "}}";
            var result = new GraphNode(pathJson);
            var path = result.ToPath();
            CollectionAssert.AreEqual(
                new string[][]
                {
                    new [] { "a" }, new string[0], new[] { "c", "d" }, new[] { "e", "f", "g" }, new string[0]
                }, path.Labels);
            Assert.AreEqual(2, path.Objects.Count);
            Assert.AreEqual("person", path.Objects.First().ToVertex().Label);
            Assert.AreEqual("knows", path.Objects.Skip(1).First().ToEdge().Label);
            //Verify implicit result
            var path2 = (Path) result;
            CollectionAssert.AreEqual(path.Labels, path2.Labels);
            Assert.AreEqual(path.Objects.Count, path2.Objects.Count);
            var path3 = (IPath) path;
            Assert.AreEqual(path.Objects.Count, path3.Objects.Count);
            var path4 = result.To<IPath>();
            Assert.AreEqual(path.Objects.Count, path4.Objects.Count);
        }

#if NET452
        [Test]
        public void Should_Be_Serializable()
        {
            var json = "{\"something\":true}";
            var result = JsonConvert.DeserializeObject<GraphNode>(json);
            Assert.True(result.Get<bool>("something"));
            Assert.AreEqual(json, JsonConvert.SerializeObject(result));

            json = "{\"something\":{\"val\":1}}";
            result = JsonConvert.DeserializeObject<GraphNode>(json);
            var objectTree = result.Get<GraphNode>("something");
            Assert.NotNull(objectTree);
            Assert.AreEqual(1D, objectTree.Get<double>("val"));
            Assert.AreEqual(json, JsonConvert.SerializeObject(result));
        }
#endif

        private static GraphNode GetGraphNode(string json)
        {
            return new GraphNode(new GraphSON1Node("{\"result\": " + json + "}"));
        }
    }
}
