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

using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.SessionManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class HeartbeatTests : TestGlobals
    {
        private SimulacronCluster _testCluster;
        private const QueryType OptionsQueryType = QueryType.Options;
        private const string Query = "SELECT id FROM dummy_table";

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "1"});
            _testCluster.PrimeFluent(b => b.WhenQuery(HeartbeatTests.Query).ThenVoidSuccess());
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _testCluster.RemoveAsync().Wait();
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task Connection_Should_Send_Options_Requests_For_Heartbeats(bool executeQuery)
        {
            var builder = ClusterBuilder()
                                 .WithPoolingOptions(PoolingOptions.Create().SetHeartBeatInterval(4000))
                                 .AddContactPoint(_testCluster.InitialContactPoint);

            using (var cluster = builder.Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var logs = await _testCluster.GetNodes().First()
                                             .GetQueriesAsync(null, OptionsQueryType).ConfigureAwait(false);

                // Test idle connection after connect and after a successful query
                if (executeQuery)
                {
                    await session.ExecuteAsync(new SimpleStatement(Query)).ConfigureAwait(false);
                }
                var initialCount = logs.Count;

                await TestHelper.RetryAssertAsync(
                    async () =>
                    {
                        logs = await _testCluster.GetNodes().First()
                                                 .GetQueriesAsync(null, OptionsQueryType).ConfigureAwait(false);
                        Assert.That(logs.Count, Is.GreaterThan(initialCount));
                    },
                    500,
                    20).ConfigureAwait(false);
            }
        }

        [Test]
        public async Task Connection_Should_Be_Closed_When_Heartbeat_Times_Out_With_Socket_Still_Open()
        {
            var builder = ClusterBuilder()
                                 .WithPoolingOptions(PoolingOptions.Create()
                                                                    .SetHeartBeatInterval(2000)
                                                                    .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                                                                    .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                                 .WithSocketOptions(new SocketOptions()
                                                     .SetReadTimeoutMillis(2000)
                                                     .SetDefunctReadTimeoutThreshold(int.MaxValue))
                                 .AddContactPoint(_testCluster.InitialContactPoint);

            using (var cluster = builder.Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                await session.ExecuteAsync(new SimpleStatement(HeartbeatTests.Query)).ConfigureAwait(false);

                var pool = ((IInternalSession)session).GetPools().Single().Value;
                var connection = pool.ConnectionsSnapshot.Single();
                Assert.IsFalse(connection.IsDisposed);

                // Simulacron keeps the TCP connection ESTABLISHED and just stops reading, so the
                // heartbeat fails with OperationTimedOutException rather than SocketException. No
                // application requests are sent after this point, so only the heartbeat itself can
                // detect the unresponsive host and close the connection.
                await _testCluster.PauseReadsAsync().ConfigureAwait(false);
                try
                {
                    await TestHelper.RetryAssertAsync(
                        () =>
                        {
                            Assert.IsTrue(connection.IsDisposed);
                            return Task.CompletedTask;
                        },
                        500,
                        20).ConfigureAwait(false);
                }
                finally
                {
                    await _testCluster.ResumeReadsAsync().ConfigureAwait(false);
                }
            }
        }
    }
}