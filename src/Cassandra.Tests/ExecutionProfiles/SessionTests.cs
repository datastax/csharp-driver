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
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;
using Cassandra.Tests.Connections;
using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class SessionTests
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreateRequestHandlerWithCorrectRequestOptions_When_ExecutionProfileIsProvided(bool async)
        {
            var requestHandlerFactoryMock = Mock.Of<IRequestHandlerFactory>();
            var requestHandlerMock = Mock.Of<IRequestHandler>();
            var hostConnectionPoolFactoryMock = Mock.Of<IHostConnectionPoolFactory>();
            var clusterMock = Mock.Of<IInternalCluster>();
            var serializer = Serializer.Default;
            var config = new TestConfigurationBuilder
            {
                RequestHandlerFactory = requestHandlerFactoryMock,
                HostConnectionPoolFactory = hostConnectionPoolFactoryMock,
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "testE", new ExecutionProfileBuilder()
                               .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                               .WithReadTimeoutMillis(1)
                               .Build() },
                    { "testE2", new ExecutionProfileBuilder().Build() }
                },
                QueryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalSerial),
                SocketOptions = new SocketOptions().SetReadTimeoutMillis(60000)
            }.Build();
            Mock.Get(requestHandlerMock).Setup(r => r.SendAsync()).Returns(Task.FromResult(new RowSet()));

            var session = new Session(clusterMock, config, null, serializer);

            Mock.Get(requestHandlerFactoryMock)
                .Setup(m => m.Create(session, serializer, It.IsAny<IStatement>(), config.RequestOptions["testE"]))
                .Returns(requestHandlerMock);

            if (async)
            {
                await session.ExecuteAsync(new SimpleStatement("test query"), "testE").ConfigureAwait(false);
            }
            else
            {
                session.Execute(new SimpleStatement("test query"), "testE");
            }

            Mock.Get(requestHandlerFactoryMock).Verify(m => m.Create(session, serializer, It.IsAny<IStatement>(), config.RequestOptions["testE"]), Times.Once);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreateRequestHandlerWithDefaultRequestOptions_When_ExecutionProfileIsNotProvided(bool async)
        {
            var requestHandlerFactoryMock = Mock.Of<IRequestHandlerFactory>();
            var requestHandlerMock = Mock.Of<IRequestHandler>();
            var hostConnectionPoolFactoryMock = Mock.Of<IHostConnectionPoolFactory>();
            var clusterMock = Mock.Of<IInternalCluster>();
            var serializer = Serializer.Default;
            var config = new TestConfigurationBuilder
            {
                RequestHandlerFactory = requestHandlerFactoryMock,
                HostConnectionPoolFactory = hostConnectionPoolFactoryMock,
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "testE", new ExecutionProfileBuilder()
                               .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                               .WithReadTimeoutMillis(1)
                               .Build() },
                    { "testE2", new ExecutionProfileBuilder().Build() }
                },
                QueryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalSerial),
                SocketOptions = new SocketOptions().SetReadTimeoutMillis(60000)
            }.Build();
            Mock.Get(requestHandlerMock).Setup(r => r.SendAsync()).Returns(Task.FromResult(new RowSet()));

            var session = new Session(clusterMock, config, null, serializer);

            Mock.Get(requestHandlerFactoryMock)
                .Setup(m => m.Create(session, serializer, It.IsAny<IStatement>(), config.DefaultRequestOptions))
                .Returns(requestHandlerMock);

            if (async)
            {
                await session.ExecuteAsync(new SimpleStatement("test query")).ConfigureAwait(false);
            }
            else
            {
                session.Execute(new SimpleStatement("test query"));
            }

            Mock.Get(requestHandlerFactoryMock).Verify(m => m.Create(session, serializer, It.IsAny<IStatement>(), config.DefaultRequestOptions), Times.Once);
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreatePrepareHandlerWithCorrectRequestOptions_When_ExecutionProfileIsProvided(bool async)
        {
            var prepareHandlerFactory = Mock.Of<IPrepareHandlerFactory>();
            var prepareHandlerMock = Mock.Of<IPrepareHandler>();
            Mock.Get(prepareHandlerMock)
                .Setup(m => m.Prepare(It.IsAny<InternalPrepareRequest>(), It.IsAny<IInternalSession>(), It.IsAny<Dictionary<IPEndPoint, Exception>>()))
                .ReturnsAsync(new PreparedStatement(null, new byte[0], string.Empty, string.Empty, Serializer.Default));
            Mock.Get(prepareHandlerMock)
                .Setup(m => m.PrepareOnTheRestOfTheNodes(It.IsAny<Cassandra.Requests.InternalPrepareRequest>(), It.IsAny<IInternalSession>()))
                .Returns(TaskHelper.Completed);
            var lbpMock = Mock.Of<ILoadBalancingPolicy>();
            Mock.Get(lbpMock)
                .Setup(lbp => lbp.NewQueryPlan(It.IsAny<string>(), It.IsAny<IStatement>()))
                .Returns(new List<Host>
                {
                    new Host(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042))
                });
            var config = new TestConfigurationBuilder
            {
                Policies = new Policies(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100), new DefaultRetryPolicy()),
                PrepareHandlerFactory = prepareHandlerFactory,
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "default", new ExecutionProfileBuilder().WithLoadBalancingPolicy(lbpMock).Build() },
                    { "testE", new ExecutionProfileBuilder()
                               .WithLoadBalancingPolicy(new RoundRobinPolicy())
                               .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                               .WithReadTimeoutMillis(1)
                               .Build() },
                    { "testE2", new ExecutionProfileBuilder().Build() }
                }
            }.Build();
            Mock.Get(prepareHandlerFactory)
                .Setup(m => m.Create(It.IsAny<Serializer>(), It.IsAny<IEnumerator<Host>>(), config.RequestOptions["testE"]))
                .Returns(prepareHandlerMock);
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(config);
            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            var session = cluster.Connect();

            if (async)
            {
                await session.PrepareAsync(prepare => prepare.WithQuery("test query").WithExecutionProfile("testE")).ConfigureAwait(false);
                await session.PrepareAsync(
                    prepare => prepare.WithQuery("test query").WithCustomPayload(new Dictionary<string, byte[]>()).WithExecutionProfile("testE")).ConfigureAwait(false);
            }
            else
            {
                session.Prepare(prepare => prepare.WithQuery("test query").WithExecutionProfile("testE"));
                session.Prepare(
                    prepare => prepare.WithQuery("test query").WithCustomPayload(new Dictionary<string, byte[]>()).WithExecutionProfile("testE"));
            }

            Mock.Get(prepareHandlerFactory)
                .Verify(m => 
                    m.Create(
                        It.IsAny<Serializer>(), 
                        It.Is<IEnumerator<Host>>(hosts => 
                            hosts.MoveNext() && hosts.Current.Address.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042)) && !hosts.MoveNext()), 
                        config.RequestOptions["testE"]), 
                    Times.Exactly(2));
        }
        
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_CreatePrepareHandlerWithDefaultRequestOptions_When_ExecutionProfileIsNotProvided(bool async)
        {
            var prepareHandlerFactory = Mock.Of<IPrepareHandlerFactory>();
            var prepareHandlerMock = Mock.Of<IPrepareHandler>();
            Mock.Get(prepareHandlerMock)
                .Setup(m => m.Prepare(It.IsAny<InternalPrepareRequest>(), It.IsAny<IInternalSession>(), It.IsAny<Dictionary<IPEndPoint, Exception>>()))
                .ReturnsAsync(new PreparedStatement(null, new byte[0], string.Empty, string.Empty, Serializer.Default));
            Mock.Get(prepareHandlerMock)
                .Setup(m => m.PrepareOnTheRestOfTheNodes(It.IsAny<Cassandra.Requests.InternalPrepareRequest>(), It.IsAny<IInternalSession>()))
                .Returns(TaskHelper.Completed);
            var lbpMock = Mock.Of<ILoadBalancingPolicy>();
            Mock.Get(lbpMock)
                .Setup(lbp => lbp.NewQueryPlan(It.IsAny<string>(), It.IsAny<IStatement>()))
                .Returns(new List<Host>
                {
                    new Host(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042))
                });
            var config = new TestConfigurationBuilder
            {
                Policies = new Policies(new RoundRobinPolicy(), new ConstantReconnectionPolicy(100), new DefaultRetryPolicy()),
                PrepareHandlerFactory = prepareHandlerFactory,
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "default", new ExecutionProfileBuilder().WithLoadBalancingPolicy(lbpMock).Build() },
                    { "testE", new ExecutionProfileBuilder()
                               .WithLoadBalancingPolicy(new RoundRobinPolicy())
                               .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                               .WithReadTimeoutMillis(1)
                               .Build() },
                    { "testE2", new ExecutionProfileBuilder().Build() }
                }
            }.Build();
            Mock.Get(prepareHandlerFactory)
                .Setup(m => m.Create(It.IsAny<Serializer>(), It.IsAny<IEnumerator<Host>>(), config.RequestOptions["default"]))
                .Returns(prepareHandlerMock);
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(config);
            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            var session = cluster.Connect();
            
            if (async)
            {
                await session.PrepareAsync("test query").ConfigureAwait(false);
                await session.PrepareAsync("test query", new Dictionary<string, byte[]>()).ConfigureAwait(false);
            }
            else
            {
                session.Prepare("test query");
                session.Prepare("test query", new Dictionary<string, byte[]>());
            }

            Mock.Get(prepareHandlerFactory)
                .Verify(m => 
                    m.Create(
                        It.IsAny<Serializer>(), 
                        It.Is<IEnumerator<Host>>(hosts => 
                            hosts.MoveNext() && hosts.Current.Address.Equals(new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042)) && !hosts.MoveNext()), 
                        config.RequestOptions["default"]), 
                    Times.Exactly(2));
        }
    }
}