//
//       Copyright (C) 2019 DataStax Inc.
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
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections;
using Cassandra.Tests.Requests;

using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class PrepareHandlerTests
    {
        //[Test]
        //public async Task Should_NotUseAnyClusterSettings_When_ExecutionProfileIsProvided()
        //{
        //    var lbpCluster = new FakeLoadBalancingPolicy();
        //    var lbp = new FakeLoadBalancingPolicy();
        //    var profile = new ExecutionProfileBuilder()
        //                                  .WithConsistencyLevel(ConsistencyLevel.All)
        //                                  .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
        //                                  .WithReadTimeoutMillis(50)
        //                                  .WithLoadBalancingPolicy(lbp)
        //                                  .Build();

        //    var mockResult = BuildPrepareHandler(
        //        builder =>
        //        {
        //            builder.QueryOptions =
        //                new QueryOptions()
        //                    .SetConsistencyLevel(ConsistencyLevel.LocalOne)
        //                    .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
        //            builder.SocketOptions =
        //                new SocketOptions().SetReadTimeoutMillis(10);
        //            builder.Policies = new Policies(
        //                lbpCluster,
        //                new ConstantReconnectionPolicy(5), 
        //                new DefaultRetryPolicy(), 
        //                NoSpeculativeExecutionPolicy.Instance, 
        //                new AtomicMonotonicTimestampGenerator());
        //        },
        //        profile);
            
        //    await mockResult.PrepareHandler.Prepare(
        //        new InternalPrepareRequest("TEST"), mockResult.Session, new Dictionary<IPEndPoint, Exception>()).ConfigureAwait(false);

        //    var results = mockResult.SendResults.ToArray();
        //    Assert.AreEqual(1, results.Length);
        //    var timeouts = results.Select(r => r.TimeoutMillis).ToList();
        //    Assert.AreEqual(50, timeouts.Distinct().Single());

        //    Assert.Greater(Interlocked.Read(ref lbp.Count), 0);
        //    Assert.AreEqual(0, Interlocked.Read(ref lbpCluster.Count));
        //}

        [Test]
        public async Task Should_UseClusterSettings_When_ProfileIsNotProvided()
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var lbp = new FakeLoadBalancingPolicy();

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
                },
                null);

            await mockResult.PrepareHandler.Prepare(
                new InternalPrepareRequest("TEST"), mockResult.Session).ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            Assert.AreEqual(1, results.Length);
            var timeouts = results.Select(r => r.TimeoutMillis).ToList();
            Assert.AreEqual(10, timeouts.Distinct().Single());
            Assert.Greater(Interlocked.Read(ref lbpCluster.Count), 0);
            Assert.AreEqual(0, Interlocked.Read(ref lbp.Count));
        }
        
        private PrepareHandlerMockResult BuildPrepareHandler(
            Action<TestConfigurationBuilder> configBuilderAct,
            IExecutionProfile profile)
        {
            var cts = new CancellationTokenSource();
            var connection = Mock.Of<IConnection>();

            // create config
            var configBuilder = new TestConfigurationBuilder
            {
                ConnectionFactory = new FakeConnectionFactory(() => connection),
                Policies = new Policies(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100), new DefaultRetryPolicy())
            };
            configBuilderAct(configBuilder);
            var config = configBuilder.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock).Setup(i => i.ContactPoints).Returns(new List<IPEndPoint>
            {
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042),
                new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042)

            });
            Mock.Get(initializerMock).Setup(i => i.GetConfiguration()).Returns(config);

            // create cluster
            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            config.Policies.LoadBalancingPolicy.Initialize(cluster);
            
            // create session
            var session = new Session(cluster, config, null, Serializer.Default);

            // create prepare handler
            var prepareHandler = new PrepareHandler(
                new Serializer(ProtocolVersion.V3),
                cluster.AllHosts().GetEnumerator());

            // create mock result object
            var mockResult = new PrepareHandlerMockResult(prepareHandler, session);

            // mock connection send
            Mock.Get(connection)
                .Setup(c => c.Send(It.IsAny<IRequest>(), It.IsAny<int>()))
                .Returns<IRequest, int>(async (req, timeout) =>
                {
                    mockResult.SendResults.Enqueue(new ConnectionSendResult { Request = req, TimeoutMillis = timeout });
                    await Task.Delay(1, cts.Token).ConfigureAwait(false);
                    return new ProxyResultResponse(
                        ResultResponse.ResultResponseKind.Void, 
                        new OutputPrepared(new byte[0], new RowSetMetadata { Columns = new CqlColumn[0] }));
                });
            Mock.Get(connection)
                .SetupGet(c => c.Address)
                .Returns(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042));

            return mockResult;
        }
        
        private class ConnectionSendResult
        {
            public IRequest Request { get; set; }

            public int TimeoutMillis { get; set; }
        }

        private class PrepareHandlerMockResult
        {
            public PrepareHandlerMockResult(PrepareHandler prepareHandler, IInternalSession session)
            {
                PrepareHandler = prepareHandler;
                Session = session;
            }

            public PrepareHandler PrepareHandler { get; }

            public ConcurrentQueue<ConnectionSendResult> SendResults { get; } = new ConcurrentQueue<ConnectionSendResult>();

            public IInternalSession Session { get; }
        }

        private class FakeLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public long Count;

            public void Initialize(ICluster cluster)
            {
            }

            public HostDistance Distance(Host host)
            {
                Interlocked.Increment(ref Count);
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                throw new NotImplementedException();
            }
        }
    }
}