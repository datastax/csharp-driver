//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture]
    public class ClusterUnitTests
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
        }

        [Test]
        public void DuplicateContactPointsShouldIgnore()
        {
            var listener = new TestTraceListener();
            Trace.Listeners.Add(listener);
            var originalLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Warning;
            try
            {
                const string ip1 = "127.100.100.100";
                const string singleUniqueIp = "127.100.100.101";
                var ip2 = new IPEndPoint(IPAddress.Parse("127.100.100.100"), 9040);
                var ip3 = IPAddress.Parse("127.100.100.100");
                var cluster = Cluster.Builder()
                                     .AddContactPoints(ip1, ip1, ip1)
                                     .AddContactPoints(ip2, ip2, ip2)
                                     // IPAddresses are converted to strings so these 3 will be equal to the previous 3
                                     .AddContactPoints(ip3, ip3, ip3)
                                     .AddContactPoint(singleUniqueIp)
                                     .Build();

                Assert.AreEqual(3, cluster.InternalRef.GetResolvedEndpoints().Count);
                Trace.Flush();
                Assert.AreEqual(5, listener.Queue.Count(msg => msg.Contains("Found duplicate contact point: 127.100.100.100. Ignoring it.")));
                Assert.AreEqual(2, listener.Queue.Count(msg => msg.Contains("Found duplicate contact point: 127.100.100.100:9040. Ignoring it.")));
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                Diagnostics.CassandraTraceSwitch.Level = originalLevel;
            }
        }

        [Test]
        public void ClusterAllHostsReturnsZeroHostsOnDisconnectedCluster()
        {
            const string ip = "127.100.100.100";
            var cluster = Cluster.Builder()
             .AddContactPoint(ip)
             .Build();
            //No ring was discovered
            Assert.AreEqual(0, cluster.AllHosts().Count);
        }

        [Test]
        public void ClusterConnectThrowsNoHostAvailable()
        {
            var cluster = Cluster.Builder()
             .AddContactPoint("127.100.100.100")
             .Build();
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect("sample_ks"));
        }

        [Test]
        public void ClusterIsDisposableAfterInitError()
        {
            const string ip = "127.100.100.100";
            var cluster = Cluster.Builder()
             .AddContactPoint(ip)
             .Build();
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
            Assert.DoesNotThrow(cluster.Dispose);
        }

        [Test]
        public void Should_Not_Leak_Connections_When_Node_Unreacheable_Test()
        {
            var socketOptions = new SocketOptions().SetReadTimeoutMillis(1).SetConnectTimeoutMillis(1);
            var builder = Cluster.Builder()
                                 .AddContactPoint(TestHelper.UnreachableHostAddress)
                                 .WithSocketOptions(socketOptions);
            const int length = 1000;
            using (var cluster = builder.Build())
            {
                decimal initialLength = GC.GetTotalMemory(true);
                for (var i = 0; i < length; i++)
                {
                    var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                    Assert.AreEqual(1, ex.Errors.Count);
                }
                GC.Collect();
                Assert.Less(GC.GetTotalMemory(true) / initialLength, 1.3M,
                    "Should not exceed a 20% (1.3) more than was previously allocated");
            }
        }
    }
}