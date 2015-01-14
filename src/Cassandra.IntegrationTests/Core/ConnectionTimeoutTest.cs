//
//      Copyright (C) 2012-2014 DataStax Inc.
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

using ﻿Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Linq.Expressions;

namespace Cassandra.IntegrationTests.Core
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
            var thrown = false;
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                           .WithQueryTimeout(500);
                builder.SocketOptions.SetConnectTimeoutMillis(500);
                var cluster = builder.Build();
                cluster.Connect();
            }
            catch (NoHostAvailableException)
            {
                thrown = true;
            }

            sw.Stop();

            Assert.True(thrown, "Expected exception");
            Assert.True(sw.Elapsed.TotalMilliseconds < 1000, "The connection timeout was not respected");

            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }

        [Test]
        public void ConnectionRejectingTimeoutTest()
        {
            var originalTraceLevel = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var sw = Stopwatch.StartNew();
            var thrown = false;
            try
            {
                var builder = new Builder().WithDefaultKeyspace("system")
                                           .AddContactPoints("127.9.9.9") // local IP that will most likely not be in use
                                           .WithQueryTimeout(500);
                builder.SocketOptions.SetConnectTimeoutMillis(500);
                var cluster = builder.Build();
                cluster.Connect();
            }
            catch (NoHostAvailableException)
            {
                thrown = true;
            }

            sw.Stop();

            Assert.True(thrown, "Expected exception");
            Assert.True(sw.Elapsed.TotalMilliseconds < 1000, "The connection timeout was not respected");

            Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
        }
    }
}