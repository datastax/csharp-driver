using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class GraphResultTests : BaseUnitTest
    {
        [Test]
        public void Constructor_Should_Throw_When_Json_Is_Null()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new GraphResult(null));
        }

        [Test]
        public void Constructor_Should_Parse_Json()
        {
            dynamic result = new GraphResult("{\"result\": \"something\"}");
            Assert.AreEqual("something", result.ToString());

            result = new GraphResult("{\"result\": {\"something\": 1.2 }}");
            Assert.AreEqual(1.2D, result.something);
        }

        [Test]
        public void Should_Return_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            dynamic result = new GraphResult("{\"result\": 1.2}");
            Assert.Throws<KeyNotFoundException>(() =>
            {
                var zeta = result.zeta;
            });

            result = new GraphResult("{\"result\": {\"something\": 1.2 }}");
            Assert.Throws<KeyNotFoundException>(() =>
            {
                var gamma = result.gamma;
            });
        }

        [Test]
        public void Should_Allow_Nested_Properties_For_Object_Trees()
        {
            dynamic result = new GraphResult("{\"result\": " +
                                             "{" +
                                                "\"something\": {\"inTheAir\": 1}," +
                                                "\"everything\": {\"isAwesome\": [1, 2, \"zeta\"]}, " +
                                                "\"a\": {\"b\": {\"c\": 0.6}} " +
                                             "}}");
            Assert.AreEqual(1, result.something.inTheAir);
            CollectionAssert.AreEqual(new object[] { 1, 2, "zeta" }, result.everything.isAwesome);
            Assert.AreEqual(0.6D, result.a.b.c);
        }

        [Test]
        public void ToDouble_Should_Convert_To_Double()
        {
            var result = new GraphResult("{\"result\": 1.9}");
            Assert.AreEqual(1.9, result.ToDouble());
        }

        [Test]
        public void ToDouble_Should_Throw_For_Non_Scalar_Values()
        {
            var result = new GraphResult("{\"result\": {\"something\": 0 }}");
            Assert.Throws<InvalidOperationException>(() => result.ToDouble());
        }

        [Test]
        public void Get_T_Should_Get_A_Typed_Value_By_Name()
        {
            var result = new GraphResult("{\"result\": {\"some\": \"value1\" }}");
            Assert.AreEqual("value1", result.Get<string>("some"));
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Object_Trees()
        {
            var result = new GraphResult("{\"result\": {\"something\": {\"is_awesome\": true} }}");
            Assert.AreEqual(true, result.Get<dynamic>("something").is_awesome);
        }

        [Test]
        public void Get_T_Should_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            var result = new GraphResult("{\"result\": {\"everything\": {\"is_awesome\": true} }}");
            Assert.Throws<KeyNotFoundException>(() => result.Get<dynamic>("what"));
        }

        [Test]
        public void Equals_Should_Return_True_For_The_Same_Json()
        {
            var result1 = new GraphResult("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result2 = new GraphResult("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result3 = new GraphResult("{\"result\": {\"other\": \"value\"}}");
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
            var result = new GraphResult("{" +
              "\"result\": {" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"label\":\"vertex\"," +
                "\"type\":\"vertex\"," +
                "\"properties\":{" +
                  "\"name\":[{\"id\":{\"local_id\":\"00000000-0000-8007-0000-000000000000\",\"~type\":\"name\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":\"j\"}]," +
                  "\"age\":[{\"id\":{\"local_id\":\"00000000-0000-8008-0000-000000000000\",\"~type\":\"age\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":34}]}" +
               "}}");
            var vertex = result.ToVertex();
            Assert.AreEqual("vertex", vertex.Label);
            dynamic id = vertex.Id;
            Assert.AreEqual(586910, id.community_id);
            Assert.AreEqual(586910, vertex.Id.Get<long>("community_id"));
            Assert.AreEqual(2, vertex.Properties.Count);
            var nameProp = vertex.Properties["name"].ToArray();
            Assert.NotNull(nameProp);
            Assert.NotNull(nameProp[0].id);
            //Is convertible
            Assert.NotNull((Vertex)result);
            //Any enumeration of graph result can be casted to vertex
            IEnumerable<GraphResult> results = new[] { result, result, result };
            foreach (Vertex v in results)
            {
                Assert.NotNull(v);
            }
        }

        [Test]
        public void ToVertex_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphResult("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToVertex());
        }

        [Test]
        public void ToEdge_Should_Convert_To_Vertex()
        {
            var result = new GraphResult("{" +
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
            //Is convertible
            Assert.NotNull((Edge)result);
            //Any enumeration of graph result can be casted to edge
            IEnumerable<GraphResult> results = new[] { result, result, result };
            foreach (Edge v in results)
            {
                Assert.NotNull(v);
            }
        }

        [Test]
        public void ToEdge_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphResult("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToEdge());
        }
    }
}
