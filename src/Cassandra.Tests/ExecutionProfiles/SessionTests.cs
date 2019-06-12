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

using System.Collections.Generic;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

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
                    { "testE", ((ExecutionProfileBuilder)new ExecutionProfileBuilder()
                                                         .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                         .WithReadTimeoutMillis(1)).Build() },
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
                    { "testE", ((ExecutionProfileBuilder)new ExecutionProfileBuilder()
                               .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                               .WithReadTimeoutMillis(1)
                               ).Build() },
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
    }
}