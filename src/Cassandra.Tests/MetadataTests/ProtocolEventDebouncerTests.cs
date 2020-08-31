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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.ProtocolEvents;
using Cassandra.Tasks;

using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.MetadataTests
{
    [TestFixture]
    public class ProtocolEventDebouncerTests
    {
        private ConcurrentQueue<Task> _tasks = new ConcurrentQueue<Task>();
        private ProtocolEventDebouncer _target;

        [TearDown]
        public void TearDown()
        {
            _target?.ShutdownAsync().GetAwaiter().GetResult();
        }
        
        [Test]
        public async Task Should_OnlyCreateOneTimerAndNotInvokeChange_When_OneGlobalEventIsScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            _target = mockResult.Debouncer;

            await _target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Once());
            });
            await Task.Delay(100).ConfigureAwait(false); // assert that no more timers are invoked
            VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Once());
        }

        [Test]
        public async Task Should_CreateTwoTimersAndDisposeFirstOne_When_TwoGlobalEventsAreScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            _target = mockResult.Debouncer;

            await _target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);
            await _target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(1, timers.Length);
                VerifyTimerChange(timers[0], 10, Times.Exactly(2));
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Exactly(2));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_OnlyCreateOneTimerAndNotInvokeChange_When_OneKeyspaceEventIsScheduled(bool refreshEvent)
        {
            var mockResult = MockTimerFactory(10, 10000);
            _target = mockResult.Debouncer;

            await _target.ScheduleEventAsync(CreateProtocolEvent("ks", refreshEvent), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Once());
            });
            await Task.Delay(100).ConfigureAwait(false); // assert that no more timers are invoked
            VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Once());
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreateTwoTimersAndDisposeFirstOne_When_TwoKeyspaceEventsAreScheduled(bool refreshEvent)
        {
            var mockResult = MockTimerFactory(10, 10000);
            _target = mockResult.Debouncer;

            await _target.ScheduleEventAsync(CreateProtocolEvent("ks", refreshEvent), false).ConfigureAwait(false);
            await _target.ScheduleEventAsync(CreateProtocolEvent("ks2", refreshEvent), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(1, timers.Length);
                VerifyTimerChange(timers[0], 10, Times.Exactly(2));
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Exactly(2));
            Assert.AreEqual(2, _target.GetQueue().Keyspaces.Count);
        }

        [Test]
        public async Task Should_DelayButNotAddKeyspaceEvent_When_AGlobalEventIsScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            _target = mockResult.Debouncer;

            await _target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);
            await _target.ScheduleEventAsync(CreateProtocolEvent("ks", true), false).ConfigureAwait(false);
            await _target.ScheduleEventAsync(CreateProtocolEvent("ks2", false), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(1, timers.Length);
                VerifyTimerChange(timers[0], 10, Times.Exactly(3));
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerChange(mockResult.Timers.Single(), 10, Times.Exactly(3));
            Assert.AreEqual(0, _target.GetQueue().Keyspaces.Count);
        }
        
        [Repeat(1000)]
        [Test]
        public async Task Should_NotInvokeKeyspaceEventHandlers_When_AKeyspaceRefreshIsScheduled()
        {
            _target = new ProtocolEventDebouncer(new TaskBasedTimerFactory(), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20));

            var callbacks = new List<Task>
            {
                _target.HandleEventAsync(CreateProtocolEvent("ks", true), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks2", true), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks", false), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks2", false), true)
            };
            var tasks = _tasks.ToArray();
            var handlerTask1 = tasks[0];
            var handlerTask2 = tasks[1];
            var handlerTask3 = tasks[2];
            var handlerTask4 = tasks[3];

            await Task.WhenAll(callbacks).ConfigureAwait(false);
            Assert.IsTrue(handlerTask1.IsCompleted);
            Assert.IsTrue(handlerTask2.IsCompleted);
            Assert.IsFalse(handlerTask3.IsCompleted);
            Assert.IsFalse(handlerTask4.IsCompleted);
        }
        
        [Repeat(1000)]
        [Test]
        public async Task Should_NotInvokeAnyEventHandlers_When_AGlobalRefreshIsScheduled()
        {
            _target = new ProtocolEventDebouncer(new TaskBasedTimerFactory(), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20));

            var callbacks = new List<Task>
            {
                _target.HandleEventAsync(CreateProtocolEvent("ks", true), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks2", true), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks", false), false),
                _target.HandleEventAsync(CreateProtocolEvent("ks2", false), false),
                _target.HandleEventAsync(CreateProtocolEvent(), false),
                _target.HandleEventAsync(CreateProtocolEvent(), true),
            };
            var tasks = _tasks.ToArray();
            var handlerTask1 = tasks[0];
            var handlerTask2 = tasks[1];
            var handlerTask3 = tasks[2];
            var handlerTask4 = tasks[3];
            var handlerTask5 = tasks[4];
            var handlerTask6 = tasks[5];

            await Task.WhenAll(callbacks).ConfigureAwait(false);
            Assert.IsFalse(handlerTask1.IsCompleted);
            Assert.IsFalse(handlerTask2.IsCompleted);
            Assert.IsFalse(handlerTask3.IsCompleted);
            Assert.IsFalse(handlerTask4.IsCompleted);

            // last global event wins
            Assert.IsFalse(handlerTask5.IsCompleted);
            Assert.IsTrue(handlerTask6.IsCompleted);
        }

        private ProtocolEvent CreateProtocolEvent(string keyspace = null, bool? isRefreshEvent = null)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.None);
            _tasks.Enqueue(tcs.Task.ContinueWith(t => t.Result, TaskScheduler.Default));
            if (keyspace == null)
            {
                return new ProtocolEvent(() =>
                {
                    tcs.SetResult(true);
                    return TaskHelper.Completed;
                });
            }

            return new KeyspaceProtocolEvent(isRefreshEvent.Value, keyspace, () =>
            {
                tcs.SetResult(true);
                return TaskHelper.Completed;
            });
        }

        private void VerifyTimerChange(ITimer timer, long? delayMs, Times times)
        {
            Mock.Get(timer).Verify(
                t => t.Change(
                    It.IsAny<Action>(),
                    delayMs == null ? It.IsAny<TimeSpan>() : TimeSpan.FromMilliseconds(delayMs.Value)),
                times);
        }

        private TimerMockResult MockTimerFactory(long delayMs, long maxDelayMs)
        {
            var mockResult = new TimerMockResult();
            var timerFactory = Mock.Of<ITimerFactory>();
            Mock.Get(timerFactory)
                .Setup(t => t.Create(It.IsAny<TaskScheduler>()))
                .Returns(() =>
                {
                    var timer = Mock.Of<ITimer>();
                    mockResult.Timers.Enqueue(timer);
                    return timer;
                });
            var target = new ProtocolEventDebouncer(timerFactory, TimeSpan.FromMilliseconds(delayMs), TimeSpan.FromMilliseconds(maxDelayMs));
            mockResult.Debouncer = target;
            mockResult.TimerFactory = timerFactory;
            return mockResult;
        }

        private class TimerMockResult
        {
            public ITimerFactory TimerFactory { get; set; }

            public ConcurrentQueue<ITimer> Timers { get; set; } = new ConcurrentQueue<ITimer>();

            public ProtocolEventDebouncer Debouncer { get; set; }
        }
    }
}