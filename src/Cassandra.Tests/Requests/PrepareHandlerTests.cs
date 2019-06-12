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
//

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections;

using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class PrepareHandlerTests
    {
        [Test]
        public async Task Should_NotSendRequestToSecondHost_When_SecondHostDoesntHavePool()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[0], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(2, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();

            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(2, pools.Count);
            Assert.AreEqual(2, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(2, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that each pool contains only one connection that was called send
            var poolConnections = pools.Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection))).ToList();
            Assert.AreEqual(2, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        [Test]
        public async Task Should_NotSendRequestToSecondHost_When_SecondHostPoolDoesNotHaveConnections()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[0], HostDistance.Local).Warmup().ConfigureAwait(false);
            mockResult.Session.GetOrCreateConnectionPool(queryPlan[1], HostDistance.Local);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            
            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            Assert.AreEqual(2, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(2, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that each pool that contains connections contains only one connection that was called send
            var poolConnections = pools.Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection))).Where(p => p.Any()).ToList();
            Assert.AreEqual(2, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        [Test]
        public async Task Should_SendRequestToAllHosts_When_AllHostsHaveConnections()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[0], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[1], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            
            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            Assert.AreEqual(3, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(3, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[1].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that each pool contains only one connection that was called send
            var poolConnections = pools.Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection))).ToList();
            Assert.AreEqual(3, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        [Test]
        public async Task Should_SendRequestToAllHosts_When_AllHostsHaveConnectionsButFirstHostDoesntHavePool()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[1], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(2, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            
            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            Assert.AreEqual(3, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(3, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[1].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that each pool contains only one connection that was called send
            var poolConnections = pools.Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection))).ToList();
            Assert.AreEqual(3, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        [Test]
        public async Task Should_SendRequestToAllHosts_When_AllHostsHaveConnectionsButFirstHostPoolDoesntHaveConnections()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[1], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(2, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            
            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            Assert.AreEqual(3, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(3, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[1].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that each pool contains only one connection that was called send
            var poolConnections = pools.Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection))).ToList();
            Assert.AreEqual(3, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        [Test]
        public async Task Should_SendRequestToFirstHostOnly_When_PrepareOnAllHostsIsFalseAndAllHostsHaveConnectionsButFirstHostPoolDoesntHaveConnections()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var mockResult = BuildPrepareHandler(
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                            .SetPrepareOnAllHosts(false);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, 
                        new ConstantReconnectionPolicy(5), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator());
                });
            // mock connection send
            mockResult.ConnectionFactory.OnCreate += connection =>
            {
                Mock.Get(connection)
                    .Setup(c => c.Send(It.IsAny<IRequest>()))
                    .Returns<IRequest>(async req =>
                    {
                        mockResult.SendResults.Enqueue(new ConnectionSendResult { Connection = connection, Request = req });
                        await Task.Delay(1).ConfigureAwait(false);
                        return new ProxyResultResponse(
                            ResultResponse.ResultResponseKind.Void,
                            new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                    });
            };
            var queryPlan = mockResult.Session.Cluster.AllHosts().ToList();
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[1], HostDistance.Local).Warmup().ConfigureAwait(false);
            await mockResult.Session.GetOrCreateConnectionPool(queryPlan[2], HostDistance.Local).Warmup().ConfigureAwait(false);
            var pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(2, pools.Count);
            var request = new InternalPrepareRequest("TEST");

            await mockResult.PrepareHandler.Prepare(
                request, 
                mockResult.Session, 
                queryPlan.GetEnumerator()).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            
            pools = mockResult.Session.GetPools().ToList();
            Assert.AreEqual(3, pools.Count);
            Assert.AreEqual(1, results.Length);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.DistanceCount), 1);
            Assert.AreEqual(Interlocked.Read(ref lbpCluster.NewQueryPlanCount), 0);
            Assert.AreEqual(3, mockResult.ConnectionFactory.CreatedConnections.Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[1].Address].Count);
            Assert.LessOrEqual(1, mockResult.ConnectionFactory.CreatedConnections[queryPlan[2].Address].Count);
            // Assert that pool of first host contains only one connection that was called send
            var poolConnections = 
                pools
                    .Select(p => p.Value.ConnectionsSnapshot.Intersect(results.Select(r => r.Connection)))
                    .Where(p => mockResult.ConnectionFactory.CreatedConnections[queryPlan[0].Address].Contains(p.SingleOrDefault()))
                    .ToList();
            Assert.AreEqual(1, poolConnections.Count);
            foreach (var pool in poolConnections)
            {
                Mock.Get(pool.Single()).Verify(c => c.Send(request), Times.Once);
            }
        }
        
        private PrepareHandlerMockResult BuildPrepareHandler(Action<TestConfigurationBuilder> configBuilderAct)
        {
            var factory = new FakeConnectionFactory(MockConnection);

            // create config
            var configBuilder = new TestConfigurationBuilder
            {
                ConnectionFactory = factory,
                Policies = new Policies(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100), new DefaultRetryPolicy())
            };
            configBuilderAct(configBuilder);
            var config = configBuilder.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock).Setup(i => i.ContactPoints).Returns(new List<IPEndPoint>
            {
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042),
                new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042),
                new IPEndPoint(IPAddress.Parse("127.0.0.3"), 9042)

            });
            Mock.Get(initializerMock).Setup(i => i.GetConfiguration()).Returns(config);

            // create cluster
            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            config.Policies.LoadBalancingPolicy.Initialize(cluster);
            
            // create session
            var session = new Session(cluster, config, null, Serializer.Default);

            // create prepare handler
            var prepareHandler = new PrepareHandler(new Serializer(ProtocolVersion.V3));

            // create mock result object
            var mockResult = new PrepareHandlerMockResult(prepareHandler, session, factory);
            
            return mockResult;
        }

        private IConnection MockConnection(IPEndPoint endpoint)
        {
            var connection = Mock.Of<IConnection>();
            
            Mock.Get(connection)
                .SetupGet(c => c.Address)
                .Returns(endpoint);

            return connection;
        }
        
        private class ConnectionSendResult
        {
            public IRequest Request { get; set; }

            public IConnection Connection { get; set; }
        }

        private class PrepareHandlerMockResult
        {
            public PrepareHandlerMockResult(PrepareHandler prepareHandler, IInternalSession session, FakeConnectionFactory factory)
            {
                PrepareHandler = prepareHandler;
                Session = session;
                ConnectionFactory = factory;
            }

            public PrepareHandler PrepareHandler { get; }

            public ConcurrentQueue<ConnectionSendResult> SendResults { get; } = new ConcurrentQueue<ConnectionSendResult>();

            public IInternalSession Session { get; }

            public FakeConnectionFactory ConnectionFactory { get; }
        }

        private class FakeLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public long DistanceCount;
            public long NewQueryPlanCount;

            public void Initialize(ICluster cluster)
            {
            }

            public HostDistance Distance(Host host)
            {
                Interlocked.Increment(ref DistanceCount);
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                Interlocked.Increment(ref NewQueryPlanCount);
                throw new NotImplementedException();
            }
        }
    }
}