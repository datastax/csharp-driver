//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq.Expressions;
using Dse.Test.Integration.TestClusterManagement;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category("short")]
    public class ConnectionTimeoutTest : TestGlobals
    {
        [Test]
        public void ConnectionDroppingTimeoutTest()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var sw = Stopwatch.StartNew();
            Assert.Throws<NoHostAvailableException>(() =>
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                           .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(700));
                var cluster = builder.Build();
                cluster.Connect();
            });
            sw.Stop();
            // Consider timer precision ~16ms
            Assert.Greater(sw.Elapsed.TotalMilliseconds, 684, "The connection timeout was not respected");
            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }
    }
}