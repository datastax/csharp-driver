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

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using Cassandra.ExecutionProfiles;
using Cassandra.Metrics;
using Cassandra.Metrics.Internal;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Observers;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Moq;

using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
    public class RequestHandlerMockTests
    {
        private static IInternalSession GetMockInternalSession()
        {
            var sessionMock = new Mock<IInternalSession>();
            var clusterMock = new Mock<IInternalCluster>();
            sessionMock.Setup(x => x.InternalCluster).Returns(clusterMock.Object);
            sessionMock.Setup(x => x.ObserverFactory)
                       .Returns(new MetricsObserverFactory(new MetricsManager(new NullDriverMetricsProvider(), new DriverMetricsOptions(), true, "s1")));
            return sessionMock.Object;
        }
        private static Configuration GetConfig(ILoadBalancingPolicy lbp)
        {
            var requestExecutionFactory = Mock.Of<IRequestExecutionFactory>();
            Mock.Get(requestExecutionFactory)
                .Setup(m => m.Create(
                    It.IsAny<IRequestHandler>(),
                    It.IsAny<IInternalSession>(),
                    It.IsAny<IRequest>(),
                    It.IsAny<IRequestObserver>()))
                .Returns(Mock.Of<IRequestExecution>());

            return new TestConfigurationBuilder
            {
                Policies = new Cassandra.Policies(lbp, null, null),
                RequestExecutionFactory = requestExecutionFactory
            }.Build();
        }

        [Test]
        public void Should_ThrowNoHostAvailableException_When_QueryPlanMoveNextReturnsFalse()
        {
            var sessionMock = GetMockInternalSession();
            var lbpMock = Mock.Of<ILoadBalancingPolicy>();
            Mock.Get(sessionMock).SetupGet(m => m.Cluster.Configuration).Returns(RequestHandlerMockTests.GetConfig(lbpMock));
            var enumerable = Mock.Of<IEnumerable<Host>>();
            var enumerator = Mock.Of<IEnumerator<Host>>();

            Mock.Get(enumerator).Setup(m => m.MoveNext()).Returns(false);
            Mock.Get(enumerable).Setup(m => m.GetEnumerator()).Returns(enumerator);
            Mock.Get(lbpMock)
                .Setup(m => m.NewQueryPlan(It.IsAny<string>(), It.IsAny<IStatement>()))
                .Returns(enumerable);
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            var sut = new RequestHandler(sessionMock, new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());
            Assert.Throws<NoHostAvailableException>(() => sut.GetNextValidHost(triedHosts));
        }

        [Test]
        public void Should_ThrowNoHostAvailableException_When_QueryPlanMoveNextReturnsTrueButCurrentReturnsNull()
        {
            var sessionMock = GetMockInternalSession();
            var lbpMock = Mock.Of<ILoadBalancingPolicy>();
            Mock.Get(sessionMock).SetupGet(m => m.Cluster.Configuration).Returns(RequestHandlerMockTests.GetConfig(lbpMock));
            var enumerable = Mock.Of<IEnumerable<Host>>();
            var enumerator = Mock.Of<IEnumerator<Host>>();

            Mock.Get(enumerator).Setup(m => m.MoveNext()).Returns(true);
            Mock.Get(enumerator).SetupGet(m => m.Current).Returns((Host)null);
            Mock.Get(enumerable).Setup(m => m.GetEnumerator()).Returns(enumerator);
            Mock.Get(lbpMock)
                .Setup(m => m.NewQueryPlan(It.IsAny<string>(), It.IsAny<IStatement>()))
                .Returns(enumerable);
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            var sut = new RequestHandler(sessionMock, new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());
            Assert.Throws<NoHostAvailableException>(() => sut.GetNextValidHost(triedHosts));
        }

        [Test]
        public void Should_ReturnHost_When_QueryPlanMoveNextReturnsTrueAndCurrentReturnsHost()
        {
            var sessionMock = GetMockInternalSession();
            var lbpMock = Mock.Of<ILoadBalancingPolicy>();
            Mock.Get(sessionMock).SetupGet(m => m.Cluster.Configuration).Returns(RequestHandlerMockTests.GetConfig(lbpMock));
            var enumerable = Mock.Of<IEnumerable<Host>>();
            var enumerator = Mock.Of<IEnumerator<Host>>();
            var host = new Host(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9047));
            Mock.Get(enumerator).Setup(m => m.MoveNext()).Returns(true);
            Mock.Get(enumerator).SetupGet(m => m.Current).Returns(host);
            Mock.Get(enumerable).Setup(m => m.GetEnumerator()).Returns(enumerator);
            Mock.Get(lbpMock)
                .Setup(m => m.NewQueryPlan(It.IsAny<string>(), It.IsAny<IStatement>()))
                .Returns(enumerable);
            Mock.Get(lbpMock).Setup(m => m.Distance(host)).Returns(HostDistance.Local);
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            var sut = new RequestHandler(sessionMock, new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());
            var validHost = sut.GetNextValidHost(triedHosts);
            Assert.NotNull(validHost);
            Assert.AreEqual(host, validHost.Host);
        }
    }
}
