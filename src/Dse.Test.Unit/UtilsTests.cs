//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class UtilsTests
    {
        [Test]
        public void ParseJsonStringMap_Should_Parse_Json_Maps()
        {
            var input = "{\"hello\": \"world\"}";
            var expected = new Dictionary<string, string> { { "hello", "world" } };
            CollectionAssert.AreEqual(
                expected.Select(kv => Tuple.Create(kv.Key, kv.Value)),
                Utils.ParseJsonStringMap(input).Select(kv => Tuple.Create(kv.Key, kv.Value)));
            input = "{\"one\": \"1\", \"two\": \"2\",\n\"three\":\t\"3\"}";
            expected = new Dictionary<string, string> { { "one", "1" }, { "two", "2" }, { "three", "3" } };
            CollectionAssert.AreEquivalent(
                expected.Select(kv => Tuple.Create(kv.Key, kv.Value)),
                Utils.ParseJsonStringMap(input).Select(kv => Tuple.Create(kv.Key, kv.Value)));
        }
    }
}
