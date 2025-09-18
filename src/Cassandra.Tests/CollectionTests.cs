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
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using CollectionAssert = NUnit.Framework.Legacy.CollectionAssert;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CollectionTests
    {
        [Test]
        public void CopyOnWriteList_Should_Add_And_Count()
        {
            var list = new CopyOnWriteList<string>();
            list.Add("one");
            list.Add("two");
            Assert.AreEqual(2, list.Count);
            CollectionAssert.AreEqual(new[] { "one", "two" }, list);
        }

        [Test]
        public void CopyOnWriteList_Should_AddRange_And_Count()
        {
            var list = new CopyOnWriteList<string>();
            list.Add("one");
            list.Add("two");
            Assert.AreEqual(2, list.Count);
            list.AddRange(new[] { "three", "four" });
            Assert.AreEqual(4, list.Count);
            CollectionAssert.AreEqual(new[] { "one", "two", "three", "four" }, list);
        }

        [Test]
        public void CopyOnWriteList_Should_Add_And_Remove()
        {
            var list = new CopyOnWriteList<string> { "one", "two", "three", "four", "five" };
            Assert.AreEqual(5, list.Count);
            list.Remove("three");
            CollectionAssert.AreEqual(new[] { "one", "two", "four", "five" }, list);
            list.Remove("one");
            CollectionAssert.AreEqual(new[] { "two", "four", "five" }, list);
            list.Remove("five");
            CollectionAssert.AreEqual(new[] { "two", "four" }, list);
            list.Remove("four");
            CollectionAssert.AreEqual(new[] { "two" }, list);
            CollectionAssert.AreEqual(new[] { "two" }, list.ToArray());
            list.Remove("two");
            CollectionAssert.AreEqual(new string[0], list);
        }

        [Test]
        public void CopyOnWriteList_Should_Allow_Parallel_Calls_To_Add()
        {
            var actions = new List<Action>();
            var list = new CopyOnWriteList<int>();
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() => { list.Add(item); });
            }

            TestHelper.ParallelInvoke(actions);
            Assert.AreEqual(100, list.Count);
            for (var i = 0; i < 100; i++)
            {
                Assert.True(list.Contains(i));
            }

            var counter = 0;
            CollectionAssert.AreEquivalent(Enumerable.Repeat(0, 100).Select(_ => counter++), list);
        }

        [Test]
        public void CopyOnWriteList_Should_Allow_Parallel_Calls_To_Remove()
        {
            var actions = new List<Action>();
            var list = new CopyOnWriteList<int>();
            for (var i = 0; i < 100; i++)
            {
                list.Add(i);
            }

            Assert.AreEqual(100, list.Count);
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() => { list.Remove(item); });
            }

            TestHelper.ParallelInvoke(actions);
            Assert.AreEqual(0, list.Count);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Add_And_Count()
        {
            var map = new CopyOnWriteDictionary<string, int>();
            map.Add("one", 1);
            map.Add("two", 2);
            Assert.AreEqual(2, map.Count);
            CollectionAssert.AreEquivalent(new[] { "one", "two" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 1, 2 }, map.Values);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Add_And_Remove()
        {
            var map = new CopyOnWriteDictionary<string, int>
            {
                { "one", 1 },
                { "two", 2 },
                { "three", 3 },
                { "four", 4 }
            };
            Assert.AreEqual(4, map.Count);
            CollectionAssert.AreEquivalent(new[] { "one", "two", "three", "four" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 1, 2, 3, 4 }, map.Values);
            map.Remove("three");
            Assert.AreEqual(3, map.Count);
            map.Remove("one");
            Assert.AreEqual(2, map.Count);
            CollectionAssert.AreEquivalent(new[] { "two", "four" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 2, 4 }, map.Values);
            map.Add("ten", 10);
            Assert.AreEqual(3, map.Count);
            CollectionAssert.AreEquivalent(new[] { "two", "four", "ten" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 2, 4, 10 }, map.Values);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Allow_Parallel_Calls_To_Add()
        {
            var actions = new List<Action>();
            var map = new CopyOnWriteDictionary<int, int>();
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() => { map.Add(item, item * 1000); });
            }

            TestHelper.ParallelInvoke(actions);
            Assert.AreEqual(100, map.Count);
            for (var i = 0; i < 100; i++)
            {
                Assert.AreEqual(i * 1000, map[i]);
            }

            var counter = 0;
            CollectionAssert.AreEquivalent(Enumerable.Repeat(0, 100).Select(_ => counter++), map.Keys);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Allow_Parallel_Calls_To_Remove()
        {
            var actions = new List<Action>();
            var map = new CopyOnWriteDictionary<int, int>();
            for (var i = 0; i < 100; i++)
            {
                map.Add(i, i * 2000);
            }

            Assert.AreEqual(100, map.Count);
            //remove everything except 0 and 1
            for (var i = 2; i < 100; i++)
            {
                var item = i;
                actions.Add(() => { map.Remove(item); });
            }

            TestHelper.ParallelInvoke(actions);
            Assert.AreEqual(2, map.Count);
            Assert.AreEqual(0, map[0]);
            Assert.AreEqual(2000, map[1]);
        }

        [Test]
        public void CopyOnWriteDictionary_GetOrAdd_Should_Return_The_Current_Value()
        {
            var map = new CopyOnWriteDictionary<string, int>();
            Assert.AreEqual(0, map.Count);
            var value = map.GetOrAdd("key1", 1);
            Assert.AreEqual(1, value);
            value = map.GetOrAdd("key1", 2);
            //not modified
            Assert.AreEqual(1, value);
            Assert.AreEqual(1, map.Count);
            Assert.AreEqual(1, map["key1"]);
            value = map.GetOrAdd("key2", 10);
            Assert.AreEqual(10, value);
            Assert.AreEqual(2, map.Count);
            Assert.AreEqual(10, map["key2"]);
        }

        internal class ShardableItem : IShardable
        {
            public int ShardID { get; }

            public string Value { get; }

            public ShardableItem(int shardId, string value)
            {
                ShardID = shardId;
                Value = value;
            }

            public override bool Equals(object obj)
            {
                if (obj is ShardableItem other)
                {
                    return ShardID == other.ShardID && Value == other.Value;
                }

                return false;
            }

            public override int GetHashCode()
            {
                return (ShardID, Value).GetHashCode();
            }
        }

        [TestFixture]
        public class ShardedListTests
        {
            [Test]
            public void EmptyConstructor_ShouldInitializeEmpty()
            {
                var list = new ShardedList<ShardableItem>();

                Assert.AreEqual(0, list.Count);
                Assert.AreEqual(0, list.Length);
                Assert.IsTrue(list.IsReadOnly);
                Assert.IsEmpty(list.GetAllItems());
                Assert.IsEmpty(list.GetPerShardSnapshot());
            }

            [Test]
            public void Constructor_WithArray_ShouldCopyItems()
            {
                var items = new[]
                {
                    new ShardableItem(1, "A"),
                    new ShardableItem(2, "B"),
                    new ShardableItem(1, "C")
                };

                var list = new ShardedList<ShardableItem>(items);

                Assert.AreEqual(3, list.Count);
                Assert.That(list.GetAllItems(), Is.EquivalentTo(items));
            }

            [Test]
            public void Indexer_ShouldReturnCorrectItem()
            {
                var items = new[]
                {
                    new ShardableItem(0, "X"),
                    new ShardableItem(1, "Y")
                };

                var list = new ShardedList<ShardableItem>(items);

                Assert.AreEqual("X", list[0].Value);
                Assert.AreEqual("Y", list[1].Value);
            }

            [Test]
            public void GetItemsForShard_ShouldReturnCorrectShardItems()
            {
                var items = new[]
                {
                    new ShardableItem(0, "First"),
                    new ShardableItem(1, "Second"),
                    new ShardableItem(0, "Third")
                };

                var list = new ShardedList<ShardableItem>(items);

                var shard0 = list.GetItemsForShard(0);
                var shard1 = list.GetItemsForShard(1);
                var shard2 = list.GetItemsForShard(2); // Should be empty

                Assert.That(shard0.Select(x => x.Value), Is.EquivalentTo(new[] { "First", "Third" }));
                Assert.That(shard1.Select(x => x.Value), Is.EquivalentTo(new[] { "Second" }));
                Assert.IsEmpty(shard2);
            }

            [Test]
            public void GetEnumerator_ShouldEnumerateItems()
            {
                var items = new[]
                {
                    new ShardableItem(1, "One"),
                    new ShardableItem(2, "Two")
                };

                var list = new ShardedList<ShardableItem>(items);

                CollectionAssert.AreEqual(items, list.ToList());
            }
        }

        [TestFixture]
        public class CopyOnWriteShardedListTests
        {
            [Test]
            public void Add_ShouldAddItem()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                list.Add(new ShardableItem(0, "Alpha"));

                Assert.AreEqual(1, list.Count);
                Assert.AreEqual("Alpha", list.GetSnapshot()[0].Value);
            }

            [Test]
            public void AddRange_ShouldAddMultipleItems()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                list.AddRange(new[]
                {
                    new ShardableItem(1, "Beta"),
                    new ShardableItem(2, "Gamma")
                });

                Assert.AreEqual(2, list.Count);
                Assert.That(list.GetSnapshot().Select(x => x.Value), Is.EquivalentTo(new[] { "Beta", "Gamma" }));
            }

            [Test]
            public void Remove_ShouldRemoveItem()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                var item = new ShardableItem(0, "Delta");
                list.Add(item);
                var removed = list.Remove(item);

                Assert.IsTrue(removed);
                Assert.AreEqual(0, list.Count);
            }

            [Test]
            public void Remove_ShouldReturnFalse_WhenItemNotFound()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                var removed = list.Remove(new ShardableItem(1, "Zeta"));

                Assert.IsFalse(removed);
            }

            [Test]
            public void Clear_ShouldEmptyTheList()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                list.Add(new ShardableItem(0, "Eta"));
                list.Add(new ShardableItem(1, "Theta"));

                list.Clear();

                Assert.AreEqual(0, list.Count);
            }

            [Test]
            public void Contains_ShouldFindItem()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();
                var item = new ShardableItem(0, "Iota");

                list.Add(item);

                Assert.IsTrue(list.Contains(item));
            }

            [Test]
            public void Contains_ShouldReturnFalse_WhenNotPresent()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                Assert.IsFalse(list.Contains(new ShardableItem(1, "Kappa")));
            }

            [Test]
            public void GetItemsForShard_ShouldReturnShardItems()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                var item1 = new ShardableItem(2, "Lambda");
                var item2 = new ShardableItem(2, "Mu");
                var item3 = new ShardableItem(3, "Nu");

                list.Add(item1);
                list.Add(item2);
                list.Add(item3);

                var shardItems = list.GetItemsForShard(2);

                Assert.That(shardItems.Select(x => x.Value), Is.EquivalentTo(new[] { "Lambda", "Mu" }));

                var shard3 = list.GetItemsForShard(3);
                Assert.That(shard3.Select(x => x.Value), Is.EquivalentTo(new[] { "Nu" }));

                var nonExistentShard = list.GetItemsForShard(5);
                Assert.IsEmpty(nonExistentShard);
            }

            [Test]
            public void CopyTo_ShouldCopyElements()
            {
                var list = new CopyOnWriteShardedList<ShardableItem>();

                list.Add(new ShardableItem(0, "Xi"));
                list.Add(new ShardableItem(0, "Omicron"));

                var array = new ShardableItem[2];
                list.CopyTo(array, 0);

                Assert.That(array.Select(x => x.Value), Is.EquivalentTo(new[] { "Xi", "Omicron" }));
            }
        }
    }
}