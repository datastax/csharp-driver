// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.ProtocolEvents;
using Cassandra.Tasks;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.MetadataTests
{
    [TestFixture]
    public class ProtocolEventDebouncerTests
    {
        private ConcurrentQueue<Task> _tasks = new ConcurrentQueue<Task>();

        [Test]
        public async Task Should_OnlyCreateOneTimerAndNotInvokeChange_When_OneGlobalEventIsScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            var target = mockResult.Debouncer;

            await target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerFactoryCreate(mockResult, 10, Times.Once());
            });
            await Task.Delay(100).ConfigureAwait(false); // assert that no more timers are invoked
            VerifyTimerFactoryCreate(mockResult, 10, Times.Once());
            VerifyTimerChange(mockResult.Timers.Single(), null, Times.Never());
        }

        [Test]
        public async Task Should_CreateTwoTimersAndDisposeFirstOne_When_TwoGlobalEventsAreScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            var target = mockResult.Debouncer;

            await target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);
            await target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(2));
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(2, timers.Length);
                VerifyTimerChange(timers[0], Timeout.Infinite, Times.Once());
                VerifyTimerChange(timers[1], Timeout.Infinite, Times.Never());
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(2));
            VerifyTimerChange(mockResult.Timers.First(), Timeout.Infinite, Times.Once());
            VerifyTimerChange(mockResult.Timers.Last(), Timeout.Infinite, Times.Never());
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_OnlyCreateOneTimerAndNotInvokeChange_When_OneKeyspaceEventIsScheduled(bool refreshEvent)
        {
            var mockResult = MockTimerFactory(10, 10000);
            var target = mockResult.Debouncer;

            await target.ScheduleEventAsync(CreateProtocolEvent("ks", refreshEvent), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerFactoryCreate(mockResult, 10, Times.Once());
            });
            await Task.Delay(100).ConfigureAwait(false); // assert that no more timers are invoked
            VerifyTimerFactoryCreate(mockResult, 10, Times.Once());
            VerifyTimerChange(mockResult.Timers.Single(), null, Times.Never());
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreateTwoTimersAndDisposeFirstOne_When_TwoKeyspaceEventsAreScheduled(bool refreshEvent)
        {
            var mockResult = MockTimerFactory(10, 10000);
            var target = mockResult.Debouncer;

            await target.ScheduleEventAsync(CreateProtocolEvent("ks", refreshEvent), false).ConfigureAwait(false);
            await target.ScheduleEventAsync(CreateProtocolEvent("ks2", refreshEvent), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(2));
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(2, timers.Length);
                VerifyTimerChange(timers[0], Timeout.Infinite, Times.Once());
                VerifyTimerChange(timers[1], Timeout.Infinite, Times.Never());
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(2));
            VerifyTimerChange(mockResult.Timers.First(), Timeout.Infinite, Times.Once());
            VerifyTimerChange(mockResult.Timers.Last(), Timeout.Infinite, Times.Never());
            Assert.AreEqual(2, target.GetQueue().Keyspaces.Count);
        }

        [Test]
        public async Task Should_DelayButNotAddKeyspaceEvent_When_AGlobalEventIsScheduled()
        {
            var mockResult = MockTimerFactory(10, 10000);
            var target = mockResult.Debouncer;

            await target.ScheduleEventAsync(CreateProtocolEvent(), false).ConfigureAwait(false);
            await target.ScheduleEventAsync(CreateProtocolEvent("ks", true), false).ConfigureAwait(false);
            await target.ScheduleEventAsync(CreateProtocolEvent("ks2", false), false).ConfigureAwait(false);

            TestHelper.RetryAssert(() =>
            {
                VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(3));
                var timers = mockResult.Timers.ToArray();
                Assert.AreEqual(3, timers.Length);
                VerifyTimerChange(timers[0], Timeout.Infinite, Times.Once());
                VerifyTimerChange(timers[1], Timeout.Infinite, Times.Once());
                VerifyTimerChange(timers[2], Timeout.Infinite, Times.Never());
            });
            await Task.Delay(100).ConfigureAwait(false);
            VerifyTimerFactoryCreate(mockResult, 10, Times.Exactly(3));
            VerifyTimerChange(mockResult.Timers.First(), Timeout.Infinite, Times.Once());
            VerifyTimerChange(mockResult.Timers.Skip(1).First(), Timeout.Infinite, Times.Once());
            VerifyTimerChange(mockResult.Timers.Last(), Timeout.Infinite, Times.Never());
            Assert.AreEqual(0, target.GetQueue().Keyspaces.Count);
        }

        private void VerifyTimerFactoryCreate(TimerMockResult mockResult, long? delayMs, Times times)
        {
            Mock.Get(mockResult.TimerFactory).Verify(
                t => t.Create(
                    It.IsAny<TimerCallback>(), 
                    It.IsAny<object>(), 
                    delayMs == null ? It.IsAny<TimeSpan>() : TimeSpan.FromMilliseconds(delayMs.Value), 
                    Timeout.InfiniteTimeSpan), 
                times);
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
                    delayMs == null ? It.IsAny<TimeSpan>() : TimeSpan.FromMilliseconds(delayMs.Value), 
                    It.IsAny<TimeSpan>()), 
                times);
        }

        private TimerMockResult MockTimerFactory(long delayMs, long maxDelayMs)
        {
            var mockResult = new TimerMockResult();
            var timerFactory = Mock.Of<ITimerFactory>();
            Mock.Get(timerFactory)
                .Setup(t => t.Create(It.IsAny<TimerCallback>(), It.IsAny<object>(), TimeSpan.FromMilliseconds(delayMs), Timeout.InfiniteTimeSpan))
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