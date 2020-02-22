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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Cassandra.Tests;

using Castle.Core;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    public class ConnectionSimulacronTests : SimulacronTest
    {
        public ConnectionSimulacronTests() : base(false, new SimulacronOptions { Nodes = "3" }, false)
        {
        }

        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_ThrowOperationTimedOut_When_ServerAppliesTcpBackPressure(bool streamMode)
        {
            SetupNewSession(b =>
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(new SocketOptions()
                                    .SetReadTimeoutMillis(1000)
                                    .SetStreamMode(streamMode)
                                    .SetDefunctReadTimeoutThreshold(int.MaxValue)));

            var maxRequestsPerConnection = Session.Cluster.Configuration
                                                  .GetOrCreatePoolingOptions(Session.Cluster.Metadata.ControlConnection.ProtocolVersion)
                                                  .GetMaxRequestsPerConnection();
            var tenKbBuffer = new byte[10240];

            await TestCluster.PauseReadsAsync().ConfigureAwait(false);

            // send number of requests = max pending
            var requests =
                Enumerable.Repeat(0, maxRequestsPerConnection * Session.Cluster.AllHosts().Count)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer))).ToList();

            var taskAll = Task.WhenAll(requests);
            try
            {
                await (await Task.WhenAny(taskAll, Task.Delay(60000)).ConfigureAwait(false)).ConfigureAwait(false);
                Assert.Fail("Should time out.");
            }
            catch (NoHostAvailableException)
            {
                // ignored
            }

            requests = requests.Where(t => t.IsFaulted).ToList();

            Assert.Greater(requests.Count, 1);

            Assert.IsTrue(requests.All(
                t => ((NoHostAvailableException)t.Exception.InnerException)
                        .Errors.Any(e => e.Value is OperationTimedOutException)));
        }

        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_RetryOnNextNodes_When_ANodeIsPaused(bool streamMode)
        {
            var pausedNode = TestCluster.GetNode(2);

            SetupNewSession(b =>
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(
                     new SocketOptions()
                         .SetReadTimeoutMillis(2000)
                         .SetStreamMode(streamMode)
                         .SetDefunctReadTimeoutThreshold(int.MaxValue)));

            var maxRequestsPerConnection =
                Session.Cluster.Configuration
                       .GetOrCreatePoolingOptions(Session.Cluster.Metadata.ControlConnection.ProtocolVersion)
                       .GetMaxRequestsPerConnection();

            var tenKbBuffer = new byte[10240];

            await pausedNode.PauseReadsAsync().ConfigureAwait(false);

            // send number of requests = max pending
            var requests =
                Enumerable.Repeat(0, maxRequestsPerConnection * Session.Cluster.AllHosts().Count)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer))).ToList();

            var pools = InternalSession.GetPools().ToList();
            var runningNodesPools = pools.Where(kvp => !kvp.Key.Equals(pausedNode.IpEndPoint));
            var pausedNodePool = pools.Single(kvp => kvp.Key.Equals(pausedNode.IpEndPoint));
            var connections = pools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();
            var runningNodesConnections = runningNodesPools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();
            var pausedNodeConnections = pausedNodePool.Value.ConnectionsSnapshot;

            await Task.WhenAll(requests).ConfigureAwait(false);

            await AssertRetryUntilWriteQueueStabilizesAsync(connections).ConfigureAwait(false);

            TestHelper.RetryAssert(
                () =>
                {
                    Assert.IsTrue(runningNodesConnections.All(c => c.InFlight == 0));
                    Assert.IsTrue(runningNodesConnections.All(c => c.WriteQueueLength == 0));
                    Assert.IsTrue(runningNodesConnections.All(c => c.PendingOperationsMapLength == 0));
                },
                100,
                100);

            Assert.IsTrue(pausedNodeConnections.All(c => c.InFlight > 0));
            Assert.IsTrue(pausedNodeConnections.All(c => c.WriteQueueLength > 0));
            Assert.IsTrue(pausedNodeConnections.All(c => c.PendingOperationsMapLength > 0));
        }

        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_ContinueRoutingTrafficToNonPausedNodes_When_ANodeIsPaused(bool streamMode)
        {
            var pausedNode = TestCluster.GetNode(2);

            const string profileName = "running-nodes";

            SetupNewSession(b =>
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(
                     new SocketOptions()
                         .SetReadTimeoutMillis(120000)
                         .SetStreamMode(streamMode))
                 .WithExecutionProfiles(opt => opt
                     .WithProfile(profileName, profile => profile
                         .WithLoadBalancingPolicy(
                             new TestBlackListLbp(
                                 Cassandra.Policies.NewDefaultLoadBalancingPolicy("dc1"))))));

            var maxRequestsPerConnection =
                Session.Cluster.Configuration
                       .GetOrCreatePoolingOptions(Session.Cluster.Metadata.ControlConnection.ProtocolVersion)
                       .GetMaxRequestsPerConnection();

            var tenKbBuffer = new byte[10240];

            await pausedNode.PauseReadsAsync().ConfigureAwait(false);

            // send number of requests = max pending
            var requests =
                Enumerable.Repeat(0, maxRequestsPerConnection * Session.Cluster.AllHosts().Count)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer))).ToList();

            try
            {
                var pools = InternalSession.GetPools().ToList();
                var runningNodesPools = pools.Where(kvp => !kvp.Key.Equals(pausedNode.IpEndPoint));
                var pausedNodePool = pools.Single(kvp => kvp.Key.Equals(pausedNode.IpEndPoint));
                var connections = pools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();
                var runningNodesConnections = runningNodesPools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();
                var pausedNodeConnections = pausedNodePool.Value.ConnectionsSnapshot;

                await AssertRetryUntilWriteQueueStabilizesAsync(connections).ConfigureAwait(false);

                TestHelper.RetryAssert(
                    () =>
                    {
                        Assert.IsTrue(runningNodesConnections.All(c => c.InFlight == 0));
                        Assert.IsTrue(runningNodesConnections.All(c => c.WriteQueueLength == 0));
                        Assert.IsTrue(runningNodesConnections.All(c => c.PendingOperationsMapLength == 0));
                    },
                    100,
                    100);

                Assert.IsTrue(pausedNodeConnections.All(c => c.InFlight > 0));
                Assert.IsTrue(pausedNodeConnections.All(c => c.WriteQueueLength > 0));
                Assert.IsTrue(pausedNodeConnections.All(c => c.PendingOperationsMapLength > 0));

                var writeQueueLengths = pausedNodeConnections.Select(c => c.WriteQueueLength);

                Assert.AreEqual(pausedNodeConnections.Sum(c => c.InFlight), requests.Count(t => !t.IsCompleted && !t.IsFaulted));

                // these should succeed because we are not hitting the paused node with the custom profile
                var moreRequests =
                    Enumerable.Range(0, 100)
                              .Select(i => Session.ExecuteAsync(
                                  new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer),
                                  profileName))
                              .ToList();

                await Task.WhenAll(moreRequests).ConfigureAwait(false);
                Assert.IsTrue(moreRequests.All(t => t.IsCompleted && !t.IsFaulted && !t.IsCanceled));
                CollectionAssert.AreEqual(writeQueueLengths, pausedNodeConnections.Select(c => c.WriteQueueLength));
            }
            finally
            {
                await TestCluster.ResumeReadsAsync().ConfigureAwait(false);
                await (await Task.WhenAny(Task.WhenAll(requests), Task.Delay(5000)).ConfigureAwait(false)).ConfigureAwait(false);
                Assert.IsTrue(requests.All(t => t.IsCompleted && !t.IsFaulted && !t.IsCanceled));
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        [Test]
        public async Task Should_KeepOperationsInWriteQueue_When_ServerAppliesTcpBackPressure(bool streamMode)
        {
            SetupNewSession(b =>
                b.WithPoolingOptions(
                     new PoolingOptions()
                         .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                         .SetMaxConnectionsPerHost(HostDistance.Local, 1))
                 .WithSocketOptions(new SocketOptions()
                                    .SetReadTimeoutMillis(120000)
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

            var failedRequests = requests.Where(t => !t.IsFaulted && !t.IsCompleted).ToList();
            Assert.Greater(failedRequests.Count, 1);
            
            var pools = InternalSession.GetPools().ToList();
            var connections = pools.SelectMany(kvp => kvp.Value.ConnectionsSnapshot).ToList();

            await AssertRetryUntilWriteQueueStabilizesAsync(connections).ConfigureAwait(false);

            Assert.IsTrue(connections.All(c => c.WriteQueueLength > 0));
            Assert.AreEqual(failedRequests.Count, connections.Sum(c => c.InFlight));
            Assert.IsTrue(failedRequests.All(t => !t.IsCompleted && !t.IsFaulted));
            var writeQueueSizes = connections.ToDictionary(c => c, c => c.WriteQueueLength, ReferenceEqualityComparer<IConnection>.Instance);

            // these should fail because we have hit max pending ops
            var moreRequests =
                Enumerable.Range(0, 100)
                          .Select(i => Session.ExecuteAsync(new SimpleStatement("INSERT INTO table1 (id) VALUES (?)", tenKbBuffer)))
                          .ToList();
            try
            {
                try
                {
                    await Task.WhenAny(Task.WhenAll(moreRequests), Task.Delay(5000)).ConfigureAwait(false);
                    Assert.Fail("Should throw exception.");
                }
                catch
                {
                    // ignored
                }
                
                var moreFailedRequests = moreRequests.Where(t => t.IsFaulted).ToList();
                Assert.IsTrue(failedRequests.All(t => !t.IsCompleted && !t.IsFaulted));
                Assert.Greater(moreFailedRequests.Count, 1);

                // in some cases (Linux) the first couple of requests complete successfully after pause-reads endpoint is invoked on simulacron
                // so a couple of the requests in the moreRequests var will be pending and the remaining will get a busypoolexception
                Assert.AreEqual(maxRequestsPerConnection * Session.Cluster.AllHosts().Count, failedRequests.Count + moreRequests.Count - moreFailedRequests.Count);
                Assert.AreEqual(maxRequestsPerConnection * Session.Cluster.AllHosts().Count, connections.Sum(c => c.InFlight));
                
                // ReSharper disable once PossibleNullReferenceException
                Assert.IsTrue(moreFailedRequests.All(t => t.IsFaulted && ((NoHostAvailableException)t.Exception.InnerException).Errors.All(e => e.Value is BusyPoolException)));
                var newWriteQueueSizes =
                    connections.ToDictionary(c => c, c => c.WriteQueueLength, ReferenceEqualityComparer<IConnection>.Instance);

                foreach (var kvp in writeQueueSizes)
                {
                    Assert.AreEqual(newWriteQueueSizes[kvp.Key], kvp.Value);
                }

                Assert.AreEqual(failedRequests.Count, connections.Sum(c => c.InFlight));
                Assert.IsTrue(connections.All(c => c.WriteQueueLength >= 1));
            }
            finally
            {
                await TestCluster.ResumeReadsAsync().ConfigureAwait(false);
                await (await Task.WhenAny(Task.WhenAll(requests.Union(moreRequests.Where(t => !t.IsCompleted && !t.IsFaulted))), Task.Delay(5000)).ConfigureAwait(false)).ConfigureAwait(false);
                Assert.IsTrue(requests.All(t => t.IsCompleted && !t.IsFaulted && !t.IsCanceled));
            }
        }

        private async Task AssertRetryUntilWriteQueueStabilizesAsync(IEnumerable<IConnection> connections, int msPerRetry = 1000, int maxRetries = 60)
        {
            foreach (var connection in connections)
            {
                var lastValue = connection.WriteQueueLength;
                await Task.Delay(msPerRetry).ConfigureAwait(false);
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
            }
        }

        private class TestBlackListLbp : ILoadBalancingPolicy
        {
            private readonly ILoadBalancingPolicy _parent;
            private readonly IPEndPoint[] _blacklisted;

            public TestBlackListLbp(ILoadBalancingPolicy parent, params IPEndPoint[] blacklisted)
            {
                _parent = parent;
                _blacklisted = blacklisted;
            }

            public void Initialize(ICluster cluster)
            {
                _parent.Initialize(cluster);
            }

            public HostDistance Distance(Host host)
            {
                return _parent.Distance(host);
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                var plan = _parent.NewQueryPlan(keyspace, query);
                return plan.Where(h => !_blacklisted.Contains(h.Address));
            }
        }

        private class NeverRetryPolicy : IExtendedRetryPolicy
        {
            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }
        }
    }
}