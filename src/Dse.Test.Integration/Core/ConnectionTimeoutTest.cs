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
using System.Threading.Tasks;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category("short")]
    public class ConnectionTimeoutTest : TestGlobals
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task ConnectionDroppingTimeoutTest(bool asyncConnection)
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var sw = Stopwatch.StartNew();
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                           .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(700));
                var cluster = builder.Build();
                await Connect(cluster, asyncConnection, session =>{}).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (NoHostAvailableException)
            {
            }
            sw.Stop();
            // Consider timer precision ~16ms
            Assert.Greater(sw.Elapsed.TotalMilliseconds, 684, "The connection timeout was not respected");
            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }
    }
}