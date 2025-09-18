//
//      Copyright (C) DataStax Inc.
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

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;
using System.Diagnostics;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
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
                var builder = ClusterBuilder().WithDefaultKeyspace("system")
                                              .AddContactPoints("1.1.1.1") // IP address that drops (not rejects !) the inbound connection
                                              .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(700));
                var cluster = builder.Build();
                await TestGlobals.ConnectAndDispose(cluster, asyncConnection, session => { }).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (NoHostAvailableException)
            {
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = originalTraceLevel;
            }
            sw.Stop();
            // Consider timer precision ~16ms
            Assert.Greater(sw.Elapsed.TotalMilliseconds, 684, "The connection timeout was not respected");
        }
    }
}