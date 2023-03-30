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
using System.Diagnostics;
using System.Linq;
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
        private static readonly Action<object> EmptyAction = _ => { };

        [Test]
        public void WheelTimer_Bucket_Should_Support_Add_And_Remove()
        {
            using (var timer = new HashedWheelTimer(500, 10))
            {
                var bucket = new Bucket();
                Assert.Null(bucket.GetHead());
                var t1 = new TimeoutItem(timer, EmptyAction, null);
                var t2 = new TimeoutItem(timer, EmptyAction, null);
                var t3 = new TimeoutItem(timer, EmptyAction, null);
                var t4 = new TimeoutItem(timer, EmptyAction, null);
                var t5 = new TimeoutItem(timer, EmptyAction, null);
                var t6 = new TimeoutItem(timer, EmptyAction, null);
                bucket.Add(t1);
                bucket.Add(t2);
                bucket.Add(t3);
                bucket.Add(t4);
                Assert.AreEqual(bucket.GetHead(), t1);
                Assert.AreEqual(bucket.GetTail(), t4);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2, t3, t4 });
                bucket.Remove(t3);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] { t1, t2, t4 });
                Assert.AreEqual(t2.Next, t4);
                Assert.AreEqual(t4.Previous, t2);
                bucket.Remove(t1);
                Assert.AreEqual(bucket.GetHead(), t2);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] { t2, t4 });
                bucket.Add(t5);
                Assert.AreEqual(bucket.GetTail(), t5);
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
        }

        [Test]
        public void WheelTimer_Bucket_Should_Remove_Head_Items_Correctly()
        {
            using (var timer = new HashedWheelTimer(500, 10))
            {
                var bucket = new Bucket();

                var t1 = new TimeoutItem(timer, EmptyAction, null);
                var t2 = new TimeoutItem(timer, EmptyAction, null);
                var t3 = new TimeoutItem(timer, EmptyAction, null);
                var t4 = new TimeoutItem(timer, EmptyAction, null);

                bucket.Add(t1);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t1});
                Assert.AreEqual(bucket.GetTail(), t1);

                bucket.Add(t2);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t1, t2});
                Assert.AreEqual(bucket.GetTail(), t2);

                bucket.Add(t3);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t1, t2, t3});
                Assert.AreEqual(bucket.GetTail(), t3);

                bucket.Add(t4);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t1, t2, t3, t4});
                Assert.AreEqual(bucket.GetTail(), t4);

                bucket.Remove(t1);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t2, t3, t4});
                Assert.AreEqual(bucket.GetTail(), t4);

                bucket.Remove(t3);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t2, t4});
                Assert.AreEqual(bucket.GetTail(), t4);

                bucket.Remove(t4);
                CollectionAssert.AreEqual(bucket.ToArray(), new[] {t2});
                Assert.AreEqual(bucket.GetTail(), t2);

                bucket.Remove(t2);
                Assert.AreEqual(bucket.ToArray(), new TimeoutItem[0]);
            }
        }

        [Test]
        public void HashedWheelTimer_Should_Schedule_In_Order()
        {
            var results = new List<int>();
            var actions = new Action[10];
            var timer = new HashedWheelTimer(200, 8);
            for (var i = 0; i < actions.Length; i++)
            {
                var index = i;
                actions[i] = () =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    timer.NewTimeout(_ =>
                    {
                        results.Add(index);
                    }, null, 200 * (actions.Length - index));
                };
            }
            TestHelper.ParallelInvoke(actions);
            var counter = 0;
            while (results.Count < actions.Length && ++counter < 20)
            {
                Thread.Sleep(500);
                Trace.WriteLine("Slept " + counter);
            }
            Assert.AreEqual(actions.Length, results.Count);
            counter = 10;
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
            using (var timer = new HashedWheelTimer(200, 8))
            {
                var flag = 0;
                timer.NewTimeout(_ => flag += 1, null, 500);
                var timeout2 = timer.NewTimeout(_ => flag += 2, null, 500);
                timer.NewTimeout(_ => flag += 4, null, 500);
                Thread.Sleep(300);
                timeout2.Cancel();
                Thread.Sleep(800);
                Assert.AreEqual(5, flag);
                timer.Dispose();
            }
        }

        [Test]
        public void HashedWheelTimer_Cancelled_Should_Be_Removed_From_Bucket()
        {
            using (var timer = new HashedWheelTimer(200, 8))
            {
                var flag = 0;
                timer.NewTimeout(_ => flag += 1, null, 500);
                //this callback should never be executed
                var timeout2 = (TimeoutItem)timer.NewTimeout(_ => flag += 2, null, 500);
                timer.NewTimeout(_ => flag += 4, null, 500);
                Thread.Sleep(300);
                Assert.NotNull(timeout2.Bucket);
                timeout2.Cancel();
                Thread.Sleep(800);
                Assert.AreEqual(5, flag);
                timer.Dispose();
            }
        }

        [Test]
        public void HashedWheelTimer_Cancelled_During_Expiry_Should_Be_Removed_From_Bucket()
        {
            UnhandledExceptionEventArgs unhandledException = null;
            UnhandledExceptionEventHandler handler = (sender, args) =>
            {
                Volatile.Write(ref unhandledException, args);
            };
            AppDomain.CurrentDomain.UnhandledException += handler;
            try
            {
                using (var timer = new HashedWheelTimer(100, 1))
                {
                    var flag = 0;
                    HashedWheelTimer.ITimeout timeout4 = null;
                    timer.NewTimeout(
                        _ =>
                        {
                            flag += 3;
                            Volatile.Read(ref timeout4).Cancel();

                        }, null, 250);
                    timer.NewTimeout(
                        _ => { flag += 7; }, null, 450);
                    var timeout5 = timer.NewTimeout(
                        _ => { flag += 17; }, null, 350);
                    Volatile.Write(ref timeout4, timeout5);
                    Thread.Sleep(500);
                    Assert.IsNull(Volatile.Read(ref unhandledException), Volatile.Read(ref unhandledException)?.ExceptionObject?.ToString());
                    Assert.AreEqual(10, flag);
                    timer.Dispose();
                }
            }
            finally
            {
                AppDomain.CurrentDomain.UnhandledException -= handler;
            }
        }
    }
}
