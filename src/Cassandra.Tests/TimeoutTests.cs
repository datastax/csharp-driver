using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.Tasks;
using NUnit.Framework;
using Bucket = Cassandra.Tasks.HashedWheelTimer.Bucket;
using TimeoutItem = Cassandra.Tasks.HashedWheelTimer.TimeoutItem;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TimeoutTests
    {
        private static readonly Action EmptyAction = () => { };

        [Test]
        public void WheelTimer_Bucket_Should_Support_Add_And_Remove()
        {
            var bucket = new Bucket();
            Assert.Null(bucket.Head);
            var t1 = new TimeoutItem(EmptyAction);
            var t2 = new TimeoutItem(EmptyAction);
            var t3 = new TimeoutItem(EmptyAction);
            var t4 = new TimeoutItem(EmptyAction);
            var t5 = new TimeoutItem(EmptyAction);
            var t6 = new TimeoutItem(EmptyAction);
            bucket.Add(t1);
            bucket.Add(t2);
            bucket.Add(t3);
            bucket.Add(t4);
            Assert.AreEqual(bucket.Head, t1);
            Assert.AreEqual(bucket.Tail, t4);
            CollectionAssert.AreEqual(bucket.ToArray(), new [] { t1, t2, t3, t4});
            bucket.Remove(t3);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2, t4 });
            Assert.AreEqual(t2.Next, t4);
            Assert.AreEqual(t4.Previous, t2);
            bucket.Remove(t1);
            Assert.AreEqual(bucket.Head, t2);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t4 });
            bucket.Add(t5);
            Assert.AreEqual(bucket.Tail, t5);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t4, t5 });
            bucket.Add(t6);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t4, t5, t6 });
            bucket.Remove(t4);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t5, t6 });
            bucket.Remove(t2);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t5, t6 });
            bucket.Remove(t6);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t5 });
            bucket.Remove(t5);
            CollectionAssert.AreEqual(bucket.ToArray(), new TimeoutItem[0]);
        }

        [Test]
        public void WheelTimer_Bucket_Should_Remove_Head_Items_Correctly()
        {
            var bucket = new Bucket();

            var t1 = new TimeoutItem(EmptyAction);
            var t2 = new TimeoutItem(EmptyAction);
            var t3 = new TimeoutItem(EmptyAction);
            var t4 = new TimeoutItem(EmptyAction);

            bucket.Add(t1);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1 });
            Assert.AreEqual(bucket.Tail, t1);

            bucket.Add(t2);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2 });
            Assert.AreEqual(bucket.Tail, t2);

            bucket.Add(t3);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2, t3 });
            Assert.AreEqual(bucket.Tail, t3);

            bucket.Add(t4);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2, t3, t4 });
            Assert.AreEqual(bucket.Tail, t4);

            bucket.Remove(t1);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t3, t4 });
            Assert.AreEqual(bucket.Tail, t4);

            bucket.Remove(t3);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t4 });
            Assert.AreEqual(bucket.Tail, t4);

            bucket.Remove(t4);
            CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2 });
            Assert.AreEqual(bucket.Tail, t2);

            bucket.Remove(t2);
            Assert.AreEqual(bucket.ToArray(), new TimeoutItem[0]);
        }

        [Test]
        public void HashedWheelTimer_Should_Schedule_In_Order()
        {
            var results = new List<int>();
            var actions = new Action[100];
            var timer = new HashedWheelTimer(20, 8);
            Stopwatch watch = new Stopwatch();
            watch.Start();
            for (var i = 0; i < actions.Length; i++)
            {
                var index = i;
                actions[i] = () =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    timer.NewTimeout(() =>
                    {
                        results.Add(index);
                    }, 20 * (actions.Length - index));
                };
            }
            TestHelper.ParallelInvoke(actions);
            var counter = 0;
            while (results.Count < actions.Length && ++counter < 10)
            {
                Thread.Sleep(500);
                Trace.WriteLine("Slept " + counter);
            }
            Assert.AreEqual(actions.Length, results.Count);
            counter = 100;
            CollectionAssert.AreEqual(Enumerable.Repeat(0, actions.Length).Select(_ => --counter), results);
            timer.Dispose();
        }

        [Test]
        public void HashedWheelTimer_Should_Not_Execute_Cancelled()
        {
            //Schedule 3 actions
            //sleep a couple of ms
            //Cancel one of them
            //Check that it has not been executed
            var timer = new HashedWheelTimer(200, 8);
            var flag = 0;
            timer.NewTimeout(() => flag += 1, 500);
            var timeout2 = timer.NewTimeout(() => flag += 2, 500);
            timer.NewTimeout(() => flag += 4, 500);
            Thread.Sleep(300);
            timeout2.Cancel();
            Thread.Sleep(800);
            Assert.AreEqual(5, flag);
            timer.Dispose();
        }
    }
}
