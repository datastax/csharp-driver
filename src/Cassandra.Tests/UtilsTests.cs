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
using System.Linq;
using Cassandra.Collections;
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
