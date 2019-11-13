//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Graph;
using NUnit.Framework;

namespace Cassandra.Tests.Graph
{
    public class GraphOptionsTests : BaseUnitTest
    {
        [Test]
        public void BuildPayload_Should_Use_Defaults()
        {
            var options = new GraphOptions();
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.AreSame(payload1, payload2);
        }

        [Test]
        public void BuildPayload_Should_Override_Default_When_Defined()
        {
            var options = new GraphOptions()
                .SetLanguage("lang1")
                .SetName("graph1")
                .SetSource("source1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("lang1"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph1"), payload1["graph-name"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.AreSame(payload1, payload2);
        }

        [Test]
        public void BuildPayload_Should_Use_Statement_Options_When_Defined()
        {
            var options = new GraphOptions()
                .SetSource("source1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphName("graph2"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph2"), payload1["graph-name"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphName("graph2"));
            Assert.AreNotSame(payload1, payload2);
        }

        [Test]
        public void BuildPayload_Should_Not_Use_Default_Name_When_IsSystemQuery()
        {
            var options = new GraphOptions()
                .SetName("graph1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetSystemQuery());
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            Assert.False(payload1.ContainsKey("graph-name"));
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetSystemQuery());
            var payload3 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.AreNotSame(payload1, payload2);
            Assert.AreNotSame(payload2, payload3);
        }

        [Test]
        public void BuildPayload_Should_Use_Same_Byte_Array_As_Default()
        {
            var options = new GraphOptions()
                .SetName("graph1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetGraphName("abc"));
            Assert.AreNotSame(payload1, payload2);
            Assert.AreSame(payload1["graph-language"], payload2["graph-language"]);
            Assert.AreSame(payload1["graph-source"], payload2["graph-source"]);
        }

        [Test]
        public void GraphStatement_Set_Methods_Test()
        {
            var statement = new SimpleGraphStatement("g.V()");
            statement
                .SetGraphSource("source1")
                .SetGraphLanguage("lang1")
                .SetGraphName("name1")
                .SetGraphReadConsistencyLevel(ConsistencyLevel.Two)
                .SetGraphWriteConsistencyLevel(ConsistencyLevel.Three);
            Assert.AreEqual("source1", statement.GraphSource);
            Assert.AreEqual("lang1", statement.GraphLanguage);
            Assert.AreEqual("name1", statement.GraphName);
            Assert.AreEqual(ConsistencyLevel.Two, statement.GraphReadConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Three, statement.GraphWriteConsistencyLevel);
        }
    }
}
