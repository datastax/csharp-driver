//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.Metrics;
using Cassandra.Metrics.Registries;
using Cassandra.Observers;
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
            var host = new Host(Address);
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
        public void Should_UseHostIdEmpty_When_HostIdIsNull()
        {
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), 9092);
            var host = new Host(hostAddress);
            var row = BuildRow(null);
            host.SetInfo(row);
            Assert.AreEqual(Guid.Empty, host.HostId);
        }
        
        private IRow BuildRow(Guid? hostId)
        {
            return new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "host_id", hostId },
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "release_version", "3.11.1" },
                { "tokens", new List<string> { "1" }}
            });
        }
    }
}
