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

        [Test]
        public void BringUpIfDown_Resets_Attempt_Reconnection_Flag()
        {
            var policy = new CountReconnectionPolicy(long.MaxValue);
            var host = new Host(Address, policy);
            host.SetDown();
            Assert.True(host.SetAttemptingReconnection());
            //Next times it should be false
            Assert.False(host.SetAttemptingReconnection());
            Assert.False(host.SetAttemptingReconnection());
            Assert.False(host.SetAttemptingReconnection());
            host.BringUpIfDown();
            //next time should be allowed
            Assert.True(host.SetAttemptingReconnection());
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
