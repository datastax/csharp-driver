using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class HostTests
    {
        private static readonly IPEndPoint Address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1000);

        [Test]
        public void SetDown_Should_Get_The_Next_Delay_Once_The_Time_Passes()
        {
            var policy = new CountReconnectionPolicy(3000);
            var host = new Host(Address, policy);
            TestHelper.ParallelInvoke(() =>
            {
                host.SetDown();
            }, 5);
            //Should call the Schedule#NextDelayMs()
            Assert.AreEqual(1, policy.CallsCount);
            //SetDown should do nothing as the time has not passed
            host.SetDown();
            Assert.AreEqual(1, policy.CallsCount);
            Thread.Sleep(3000);
            Assert.True(host.IsConsiderablyUp);
            //The nextUpTime passed
            TestHelper.ParallelInvoke(() =>
            {
                host.SetDown();
            }, 5);
            Assert.AreEqual(2, policy.CallsCount);
        }

        [Test]
        public void BringUpIfDown_Should_Allow_Multiple_Concurrent_Calls()
        {
            var policy = new CountReconnectionPolicy(100);
            var host = new Host(Address, policy);
            var counter = 0;
            host.Up += _ => Interlocked.Increment(ref counter);
            host.SetDown();
            TestHelper.ParallelInvoke(() =>
            {
                host.BringUpIfDown();
            }, 100);
            //Should fire event only once
            Assert.AreEqual(1, counter);
        }

        private class CountReconnectionPolicy : IReconnectionPolicy
        {
            private int _counter;
            private readonly long _delay;

            public int CallsCount
            {
                get { return _counter; }
            }

            public CountReconnectionPolicy(long delay)
            {
                _delay = delay;
            }

            public IReconnectionSchedule NewSchedule()
            {
                return new CountReconnectionSchedule(_delay, this);
            }

            private class CountReconnectionSchedule : IReconnectionSchedule
            {
                private readonly long _delay;
                private readonly CountReconnectionPolicy _parent;

                public CountReconnectionSchedule(long delay, CountReconnectionPolicy parent)
                {
                    _delay = delay;
                    _parent = parent;
                }

                public long NextDelayMs()
                {
                    Interlocked.Increment(ref _parent._counter);
                    return _delay;
                }
            }
        }
    }
}
