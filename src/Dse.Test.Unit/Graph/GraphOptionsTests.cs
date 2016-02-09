using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit.Graph
{
    public class GraphOptionsTests : BaseUnitTest
    {
        [Test]
        public void BuildPayload_Should_Use_Defaults()
        {
            var options = new GraphOptions();
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("default"), payload1["graph-source"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.AreSame(payload1, payload2);
        }

        [Test]
        public void BuildPayload_Should_Override_Default_When_Defined()
        {
            var options = new GraphOptions()
                .SetAlias("alias1")
                .SetLanguage("lang1")
                .SetName("graph1")
                .SetSource("source1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("lang1"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph1"), payload1["graph-name"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("alias1"), payload1["graph-alias"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.AreSame(payload1, payload2);
        }

        [Test]
        public void BuildPayload_Should_Use_Statement_Options_When_Defined()
        {
            var options = new GraphOptions()
                .SetSource("source1")
                .SetAlias("alias1");
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphAlias("alias2")
                .SetGraphName("graph2"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph2"), payload1["graph-name"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("alias2"), payload1["graph-alias"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphAlias("alias2")
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
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("default"), payload1["graph-source"]);
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
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetGraphAlias("alias1"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("default"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("alias1"), payload1["graph-alias"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetGraphAlias("alias2"));
            Assert.AreNotSame(payload1, payload2);
            Assert.AreSame(payload1["graph-language"], payload2["graph-language"]);
            Assert.AreSame(payload1["graph-source"], payload2["graph-source"]);
        }
    }
}
