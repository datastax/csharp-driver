using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Moq;
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

        [Test]
        public void HostConnectionPool_MinInFlight_Returns_The_Min_Inflight_From_Two_Connections()
        {
            var connections = new[]
            {
                GetConnectionMock(0),
                GetConnectionMock(1),
                GetConnectionMock(1),
                GetConnectionMock(10),
                GetConnectionMock(1),
            };
            var index = 1;
            var c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 2);
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 3);
            //previous had less in flight
            Assert.AreSame(connections[2], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 4);
            Assert.AreSame(connections[4], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 5);
            Assert.AreSame(connections[0], c);
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 6);
            Assert.AreSame(connections[0], c);
            index = 9;
            c = HostConnectionPool.MinInFlight(connections, ref index);
            Assert.AreEqual(index, 10);
            Assert.AreSame(connections[0], c);
        }

        private static Connection GetConnectionMock(int inflight)
        {
            var config = new Configuration();
            config.BufferPool = new Microsoft.IO.RecyclableMemoryStreamManager();
            var connectionMock = new Mock<Connection>(MockBehavior.Strict, (byte) 4, Address, config);
            connectionMock.Setup(c => c.InFlight).Returns(inflight);
            return connectionMock.Object;
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
