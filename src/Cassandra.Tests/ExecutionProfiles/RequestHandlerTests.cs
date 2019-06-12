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
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.Tests.Connections;
using Cassandra.Tests.Requests;

using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class RequestHandlerTests
    {
        [Test]
        [TestCase(RequestTypeTestCase.Batch)]
        [TestCase(RequestTypeTestCase.Simple)]
        [TestCase(RequestTypeTestCase.Bound)]
        public async Task Should_UseStatementSettings_When_StatementSettingsAreSet(RequestTypeTestCase requestType)
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var sepCluster = new FakeSpeculativeExecutionPolicy();
            var rpCluster = new FakeRetryPolicy();
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var rp = new FakeRetryPolicy();
            var rpStatement = new FakeRetryPolicy();
            var profile = ((ExecutionProfileBuilder)new ExecutionProfileBuilder()
                                          .WithConsistencyLevel(ConsistencyLevel.All)
                                          .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                          .WithReadTimeoutMillis(50)
                                          .WithLoadBalancingPolicy(lbp)
                                          .WithSpeculativeExecutionPolicy(sep)
                                          .WithRetryPolicy(rp)
                                          ).Build();

            var mockResult = BuildRequestHandler(
                BuildStatement(requestType)
                    .SetIdempotence(true)
                    .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                    .SetReadTimeoutMillis(400)
                    .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                    .SetRetryPolicy(rpStatement),
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, new ConstantReconnectionPolicy(5), rpCluster, sepCluster, new AtomicMonotonicTimestampGenerator());
                },
                profile);

            await mockResult.RequestHandler.SendAsync().ConfigureAwait(false);
            
            var results = mockResult.SendResults.ToArray();
            Assert.GreaterOrEqual(results.Length, 1);
            var timeouts = results.Select(r => r.TimeoutMillis).ToList();
            Assert.Greater(results.Length, 0);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, GetConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(ConsistencyLevel.LocalSerial, GetSerialConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(400, timeouts.Distinct().Single());

            Assert.Greater(Interlocked.Read(ref lbp.Count), 0);
            Assert.Greater(Interlocked.Read(ref sep.Count), 0);
            Assert.Greater(Interlocked.Read(ref rpStatement.Count), 0);
            Assert.AreEqual(0, Interlocked.Read(ref lbpCluster.Count));
            Assert.AreEqual(0, Interlocked.Read(ref sepCluster.Count));
            Assert.AreEqual(0, Interlocked.Read(ref rpCluster.Count));
            Assert.AreEqual(0, Interlocked.Read(ref rp.Count));
        }

        [Test]
        [TestCase(RequestTypeTestCase.Batch)]
        [TestCase(RequestTypeTestCase.Simple)]
        [TestCase(RequestTypeTestCase.Bound)]
        public async Task Should_NotUseAnyClusterSettings_When_ExecutionProfileIsProvided(RequestTypeTestCase requestType)
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var sepCluster = new FakeSpeculativeExecutionPolicy();
            var rpCluster = new FakeRetryPolicy();
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var rp = new FakeRetryPolicy();
            var profile = ((ExecutionProfileBuilder)new ExecutionProfileBuilder()
                                          .WithConsistencyLevel(ConsistencyLevel.All)
                                          .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                          .WithReadTimeoutMillis(50)
                                          .WithLoadBalancingPolicy(lbp)
                                          .WithSpeculativeExecutionPolicy(sep)
                                          .WithRetryPolicy(rp)
                                          ).Build();

            var mockResult = BuildRequestHandler(
                BuildStatement(requestType).SetIdempotence(true),
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, new ConstantReconnectionPolicy(5), rpCluster, sepCluster, new AtomicMonotonicTimestampGenerator());
                },
                profile);

            await mockResult.RequestHandler.SendAsync().ConfigureAwait(false);
            
            var results = mockResult.SendResults.ToArray();
            Assert.GreaterOrEqual(results.Length, 1);
            var timeouts = results.Select(r => r.TimeoutMillis).ToList();
            Assert.Greater(results.Length, 0);
            Assert.AreEqual(ConsistencyLevel.All, GetConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(ConsistencyLevel.Serial, GetSerialConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(50, timeouts.Distinct().Single());

            Assert.Greater(Interlocked.Read(ref lbp.Count), 0);
            Assert.Greater(Interlocked.Read(ref sep.Count), 0);
            Assert.Greater(Interlocked.Read(ref rp.Count), 0);
            Assert.AreEqual(0, Interlocked.Read(ref lbpCluster.Count));
            Assert.AreEqual(0, Interlocked.Read(ref sepCluster.Count));
            Assert.AreEqual(0, Interlocked.Read(ref rpCluster.Count));
        }
        
        [Test]
        [TestCase(RequestTypeTestCase.Batch)]
        [TestCase(RequestTypeTestCase.Simple)]
        [TestCase(RequestTypeTestCase.Bound)]
        public async Task Should_UseClusterSettings_When_ProfileIsNotProvided(RequestTypeTestCase requestType)
        {
            var lbpCluster = new FakeLoadBalancingPolicy();
            var sepCluster = new FakeSpeculativeExecutionPolicy();
            var rpCluster = new FakeRetryPolicy();
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var rp = new FakeRetryPolicy();

            var mockResult = BuildRequestHandler(
                BuildStatement(requestType).SetIdempotence(true),
                builder =>
                {
                    builder.QueryOptions =
                        new QueryOptions()
                            .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                            .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
                    builder.SocketOptions =
                        new SocketOptions().SetReadTimeoutMillis(10);
                    builder.Policies = new Policies(
                        lbpCluster, new ConstantReconnectionPolicy(5), rpCluster, sepCluster, new AtomicMonotonicTimestampGenerator());
                },
                null);

            await mockResult.RequestHandler.SendAsync().ConfigureAwait(false);

            var results = mockResult.SendResults.ToArray();
            Assert.GreaterOrEqual(results.Length, 1);
            var timeouts = results.Select(r => r.TimeoutMillis).ToList();
            Assert.Greater(results.Length, 0);
            Assert.AreEqual(ConsistencyLevel.LocalOne, GetConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(ConsistencyLevel.LocalSerial, GetSerialConsistencyLevels(requestType, results).Distinct().Single());
            Assert.AreEqual(10, timeouts.Distinct().Single());
            Assert.Greater(Interlocked.Read(ref lbpCluster.Count), 0);
            Assert.Greater(Interlocked.Read(ref sepCluster.Count), 0);
            Assert.Greater(Interlocked.Read(ref rpCluster.Count), 0);
            Assert.AreEqual(0, Interlocked.Read(ref lbp.Count));
            Assert.AreEqual(0, Interlocked.Read(ref sep.Count));
            Assert.AreEqual(0, Interlocked.Read(ref rp.Count));
        }

        private IEnumerable<ConsistencyLevel> GetConsistencyLevels(RequestTypeTestCase testCase, ConnectionSendResult[] results)
        {
            switch (testCase)
            {
                case RequestTypeTestCase.Batch:
                    return results.Select(r => ((BatchRequest) r.Request).Consistency);
                case RequestTypeTestCase.Bound:
                    return results.Select(r => ((ExecuteRequest) r.Request).Consistency);
                case RequestTypeTestCase.Simple:
                    return results.Select(r => ((QueryRequest) r.Request).Consistency);
                default:
                    throw new InvalidOperationException();
            }
        }
        
        private IEnumerable<ConsistencyLevel> GetSerialConsistencyLevels(RequestTypeTestCase testCase, ConnectionSendResult[] results)
        {
            switch (testCase)
            {
                case RequestTypeTestCase.Batch:
                    return results.Select(r => ((BatchRequest) r.Request).SerialConsistency);
                case RequestTypeTestCase.Bound:
                    return results.Select(r => ((ExecuteRequest) r.Request).SerialConsistency);
                case RequestTypeTestCase.Simple:
                    return results.Select(r => ((QueryRequest) r.Request).SerialConsistency);
                default:
                    throw new InvalidOperationException();
            }
        }
        
        private IStatement BuildStatement(RequestTypeTestCase testCase)
        {
            switch (testCase)
            {
                case RequestTypeTestCase.Batch:
                    return new BatchStatement();
                case RequestTypeTestCase.Bound:
                    var mockPs = Mock.Of<PreparedStatement>();
                    var bs = new BoundStatement(mockPs);
                    return bs;
                case RequestTypeTestCase.Simple:
                    return new SimpleStatement("select");
                default:
                    throw new InvalidOperationException();
            }
        }

        private RequestHandlerMockResult BuildRequestHandler(
            IStatement statement,
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

            // create request handler
            var options = profile != null
                ? new RequestOptions(profile, null, config.Policies, config.SocketOptions, config.QueryOptions, config.ClientOptions)
                : config.DefaultRequestOptions;
            var requestHandler = new RequestHandler(
                session,
                new Serializer(ProtocolVersion.V3),
                statement,
                options);

            // create mock result object
            var mockResult = new RequestHandlerMockResult(requestHandler);

            // mock connection send
            Mock.Get(connection)
                .Setup(c => c.Send(It.IsAny<IRequest>(), It.IsAny<Action<Exception, Response>>(), It.IsAny<int>()))
                .Returns<IRequest, Action<Exception, Response>, int>((req, act, timeout) =>
                {
                    mockResult.SendResults.Enqueue(new ConnectionSendResult { Request = req, TimeoutMillis = timeout });
                    Task.Run(async () =>
                    {
                        var rp = (FakeRetryPolicy) (statement.RetryPolicy ?? options.RetryPolicy);
                        var sep = (FakeSpeculativeExecutionPolicy) options.SpeculativeExecutionPolicy;
                        if (Interlocked.Read(ref rp.Count) > 0 && Interlocked.Read(ref sep.Count) > 0)
                        {
                            await Task.Delay(1, cts.Token).ConfigureAwait(false);
                            act(null, new ProxyResultResponse(ResultResponse.ResultResponseKind.Void));
                            cts.Cancel();
                        }
                        else
                        {
                            try
                            {
                                await Task.Delay(10, cts.Token).ConfigureAwait(false);
                            }
                            finally
                            {
                                act(new OverloadedException(string.Empty), null);
                            }
                        }
                    });
                    return new OperationState(act)
                    {
                        Request = req,
                        TimeoutMillis = timeout
                    };
                });
            Mock.Get(connection)
                .SetupGet(c => c.Address)
                .Returns(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042));

            return mockResult;
        }

        public enum RequestTypeTestCase
        {
            Simple,
            Batch,
            Bound
        }

        private class ConnectionSendResult
        {
            public IRequest Request { get; set; }

            public int TimeoutMillis { get; set; }
        }

        private class RequestHandlerMockResult
        {
            public RequestHandlerMockResult(IRequestHandler requestHandler)
            {
                RequestHandler = requestHandler;
            }

            public IRequestHandler RequestHandler { get; }

            public ConcurrentQueue<ConnectionSendResult> SendResults { get; } = new ConcurrentQueue<ConnectionSendResult>();
        }

        private class FakeLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public long Count;

            public void Initialize(ICluster cluster)
            {
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                Interlocked.Increment(ref Count);
                return new List<Host>
                {
                    new Host(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042)),
                    new Host(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042)) // 2 hosts for speculative execution policy
                };
            }
        }

        private class FakeRetryPolicy : IExtendedRetryPolicy
        {
            public long Count;

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                Interlocked.Increment(ref Count);
                if (Interlocked.Read(ref Count) > 1)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                Interlocked.Increment(ref Count);
                if (Interlocked.Read(ref Count) > 1000)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                Interlocked.Increment(ref Count);
                if (Interlocked.Read(ref Count) > 1000)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(cl);
                }
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                Interlocked.Increment(ref Count);
                if (Interlocked.Read(ref Count) > 1000)
                {
                    return RetryDecision.Rethrow();
                }
                else
                {
                    return RetryDecision.Retry(null);
                }
            }
        }

        private class FakeSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
        {
            public long Count;

            public void Dispose()
            {
            }

            public void Initialize(ICluster cluster)
            {
            }

            public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
            {
                Interlocked.Increment(ref Count);
                return new ConstantSpeculativeExecutionPolicy(10, 1).NewPlan(keyspace, statement);
            }
        }
    }
}