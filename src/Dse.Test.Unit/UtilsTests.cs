//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Collections;
using NUnit.Framework;

namespace Dse.Test.Unit
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

        [Test]
        public void ByteArrayComparer_GetHashCode_Test()
        {
            var comparer = new ByteArrayComparer();
            var items = new[]
            {
                Tuple.Create(new byte[]{ 1, 2, 3 }, new byte[]{ 1, 2, 3 }, true),
                Tuple.Create(new byte[]{ 1, 2, 3 }, new byte[]{ 1, 2 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4 }, new byte[]{ 1, 2, 3, 4 }, true),
                Tuple.Create(new byte[]{ 1, 2, 3, 4 }, new byte[]{ 0, 2, 3, 41 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8}, true),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[]{ 1, 2, 3, 4, 5, 6, 7, 9}, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[]{ 1, 2, 3, 4, 4, 6, 7, 8}, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, true),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, new byte[]{ 1, 2, 3, 4, 5, 6, 7, 9, 9, 10 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 11 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 
                    new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }, true),
                Tuple.Create(new byte[]{ 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 160}, 
                    new byte[]{ 10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 160}, true),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 
                             new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 
                             new byte[]{ 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }, false),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 10 }, new byte[]{ 1, 2, 3, 4, 5, 10 }, true),
                Tuple.Create(new byte[]{ 1, 2, 3, 4, 5, 10 }, new byte[]{ 2, 2, 3, 4, 5, 11 }, false)
            };

            foreach (var item in items)
            {
                if (item.Item3)
                {
                    Assert.AreEqual(comparer.GetHashCode(item.Item1), comparer.GetHashCode(item.Item2));
                }
                else
                {
                    Assert.AreNotEqual(comparer.GetHashCode(item.Item1), comparer.GetHashCode(item.Item2), 
                        "For value: " + string.Join(", ", item.Item1));
                }
            }
        }
    }
}
