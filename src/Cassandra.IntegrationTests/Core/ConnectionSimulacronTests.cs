//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Linq;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Tasks;
using Cassandra.Tests;

using Castle.Core;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    public class ConnectionSimulacronTests : SimulacronTest
    {
        protected override Builder ConfigBuilder(Builder b)
        {
            return b.WithPoolingOptions(
                        new PoolingOptions()
                            .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                            .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                    .WithSocketOptions(new SocketOptions()
                        .SetReadTimeoutMillis(0)
                        .SetStreamMode(true));
        }
        
        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_ThrowOperationTimedOut_When_ServerAppliesTcpBackpressure(bool streamMode)
        {
            SetupNewSession(b => 
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(new SocketOptions()
                                    .SetReadTimeoutMillis(3000)
                                    .SetStreamMode(streamMode)));

            var maxRequestsPerConnection = Session.Cluster.Configuration
                                                  .GetOrCreatePoolingOptions(Session.Cluster.Metadata.ControlConnection.ProtocolVersion)
                                                  .GetMaxRequestsPerConnection();
            var tenKbBuffer = new byte[10240];

            await TestCluster.PauseReadsAsync().ConfigureAwait(false);

            // send number of requests = max pending
            var requests =
                Enumerable.Repeat(0, maxRequestsPerConnection * Session.Cluster.AllHosts().Count)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer))).ToList();

            try
            {
                try
                {
                    await Task.WhenAny(Task.WhenAll(requests), Task.Delay(10000)).ConfigureAwait(false);
                    Assert.Fail("Should time out.");
                }
                catch
                {
                    // ignored
                }

                Assert.IsTrue(requests.All(t => t.IsFaulted && ((NoHostAvailableException)t.Exception.InnerException).Errors.Single().Value is OperationTimedOutException));
            }
            finally
            {
                await TestCluster.ResumeReadsAsync().ConfigureAwait(false);
                await Task.WhenAny(Task.WhenAll(requests), Task.Delay(5000)).ConfigureAwait(false);
                Assert.IsTrue(requests.All(t => t.IsCompleted || t.IsFaulted || t.IsCanceled));
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_KeepOperationsInWriteQueue_When_ServerAppliesTcpBackpressure(bool streamMode)
        {
            SetupNewSession(b => 
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(new SocketOptions()
                                    .SetReadTimeoutMillis(0)
                                    .SetStreamMode(streamMode)));

            var maxRequestsPerConnection = Session.Cluster.Configuration
                                                  .GetOrCreatePoolingOptions(Session.Cluster.Metadata.ControlConnection.ProtocolVersion)
                                                  .GetMaxRequestsPerConnection();
            var tenKbBuffer = new byte[10240];

            await TestCluster.PauseReadsAsync().ConfigureAwait(false);

            // send number of requests = max pending
            var requests =
                Enumerable.Repeat(0, maxRequestsPerConnection * Session.Cluster.AllHosts().Count)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer))).ToList();

            try
            {
                var pools = InternalSession.GetPools().ToList();
                var connection = pools.Single().Value.ConnectionsSnapshot.Single();

                await AssertRetryUntilWriteQueueStabilizesAsync(connection).ConfigureAwait(false);

                Assert.AreEqual(requests.Count, connection.InFlight);
                Assert.IsTrue(requests.All(t => !t.IsCompleted && !t.IsFaulted));
                var connections = pools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();
                var writeQueueSizes = connections.ToDictionary(c => c, c => c.WriteQueueLength, ReferenceEqualityComparer<IConnection>.Instance);

                // these should fail because we have hit max pending ops
                var moreRequests =
                    Enumerable.Range(0, 1000)
                              .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer)))
                              .ToList();

                try
                {
                    await Task.WhenAny(Task.WhenAll(moreRequests), Task.Delay(5000)).ConfigureAwait(false);
                    Assert.Fail("Should throw exception.");
                }
                catch
                {
                    // ignored
                }

                Assert.IsTrue(requests.All(t => !t.IsCompleted && !t.IsFaulted));
                // ReSharper disable once PossibleNullReferenceException
                Assert.IsTrue(moreRequests.All(t => t.IsFaulted && ((NoHostAvailableException)t.Exception.InnerException).Errors.Single().Value is BusyPoolException));
                var newWriteQueueSizes =
                    connections.ToDictionary(c => c, c => c.WriteQueueLength, ReferenceEqualityComparer<IConnection>.Instance);

                foreach (var kvp in writeQueueSizes)
                {
                    Assert.AreEqual(newWriteQueueSizes[kvp.Key], kvp.Value);
                }

                Assert.AreEqual(requests.Count, connections.Sum(c => c.InFlight));
                Assert.Greater(connection.WriteQueueLength, 1);
            }
            finally
            {
                await TestCluster.ResumeReadsAsync().ConfigureAwait(false);
                await Task.WhenAny(Task.WhenAll(requests), Task.Delay(5000)).ConfigureAwait(false);
                Assert.IsTrue(requests.All(t => t.IsCompleted && !t.IsFaulted && !t.IsCanceled));
            }
        }

        private async Task AssertRetryUntilWriteQueueStabilizesAsync(IConnection connection, int msPerRetry = 1000, int maxRetries = 60)
        {
            var lastValue = connection.WriteQueueLength;
            await Task.Delay(1000).ConfigureAwait(false);
            await TestHelper.RetryAssertAsync(
                () =>
                {
                    var currentValue = connection.WriteQueueLength;
                    var tempLastValue = lastValue;
                    lastValue = currentValue;

                    Assert.AreEqual(tempLastValue, currentValue);
                    return TaskHelper.Completed;
                },
                msPerRetry,
                maxRetries);
            Assert.Greater(connection.WriteQueueLength, 1);
        }
    }
}