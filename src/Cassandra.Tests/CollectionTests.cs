using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Collections;
using NUnit.Framework;

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
        public void CopyOnWriteList_Should_Add_And_Remove()
        {
            var list = new CopyOnWriteList<string> {"one", "two", "three", "four", "five"};
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
                actions.Add(() =>
                {
                    list.Add(item);
                });
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
                actions.Add(() =>
                {
                    list.Remove(item);
                });
            }
            TestHelper.ParallelInvoke(actions);
            Assert.AreEqual(0, list.Count);
        }
    }
}
