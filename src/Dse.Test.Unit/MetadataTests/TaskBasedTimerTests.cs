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
using System.Threading;
using System.Threading.Tasks;
using Dse.ProtocolEvents;
using NUnit.Framework;

namespace Dse.Test.Unit.MetadataTests
{
    [TestFixture]
    public class TaskBasedTimerTests
    {
        private long _counter;
        private TaskScheduler _scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;

        private void WrapExclusiveScheduler(Action act)
        {
            Task.Factory.StartNew(act, CancellationToken.None, TaskCreationOptions.DenyChildAttach, _scheduler).GetAwaiter().GetResult();
        }

        [SetUp]
        public void SetUp()
        {
            Interlocked.Exchange(ref _counter, 0);
            _scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        }

        [Test]
        public async Task Should_InvokeActOnce_When_ChangeIsCalledTwice()
        {
            var target = new TaskBasedTimer(_scheduler);

            WrapExclusiveScheduler(() => target.Change(() => Interlocked.Increment(ref _counter), TimeSpan.FromMilliseconds(60000)));
            WrapExclusiveScheduler(() => target.Change(() => Interlocked.Increment(ref _counter), TimeSpan.FromMilliseconds(1)));

            TestHelper.RetryAssert(() => Assert.AreEqual(1, Interlocked.Read(ref _counter)));
            await Task.Delay(100).ConfigureAwait(false);
            Assert.AreEqual(1, Interlocked.Read(ref _counter));
        }
        
        [Test]
        public async Task Should_NotInvokeAct_When_CancelIsCalled()
        {
            var target = new TaskBasedTimer(_scheduler);

            WrapExclusiveScheduler(() => target.Change(() => Interlocked.Increment(ref _counter), TimeSpan.FromMilliseconds(500)));
            WrapExclusiveScheduler(() => target.Cancel());

            await Task.Delay(1000).ConfigureAwait(false);
            Assert.AreEqual(0, Interlocked.Read(ref _counter));
        }
        
        [Test]
        public async Task Should_InvokeActOnce_When_ChangeIsCalled()
        {
            var target = new TaskBasedTimer(_scheduler);

            WrapExclusiveScheduler(() => target.Change(() => Interlocked.Increment(ref _counter), TimeSpan.FromMilliseconds(100)));

            await Task.Delay(500).ConfigureAwait(false);
            Assert.AreEqual(1, Interlocked.Read(ref _counter));
        }
    }
}