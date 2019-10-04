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
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Serialization;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class HostConnectionPoolTests
    {
        private Host _host;
        private IEndPointResolver _resolver;

        [Test]
        public async Task Should_ResolveHostWithRefresh_When_Reconnection()
        {
            var target = CreatePool();
            Assert.AreEqual(0, target.OpenConnections);

            // create connection (which triggers a second connection creation in the background)
            var c = await target.BorrowConnectionAsync().ConfigureAwait(false);
            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(2, target.OpenConnections);
            });
            Mock.Get(_resolver).Verify(resolver => resolver.GetConnectionEndPointAsync(_host, false), Times.Exactly(2));
            Mock.Get(_resolver).Verify(resolver => resolver.GetConnectionEndPointAsync(_host, true), Times.Never);

            // remove connection to trigger reconnection
            target.Remove(c);

            TestHelper.RetryAssert(() =>
            {
                Assert.AreEqual(2, target.OpenConnections);
            });
            Mock.Get(_resolver).Verify(resolver => resolver.GetConnectionEndPointAsync(_host, false), Times.Exactly(2));
            Mock.Get(_resolver).Verify(resolver => resolver.GetConnectionEndPointAsync(_host, true), Times.Once);
        }

        private IHostConnectionPool CreatePool()
        {
            _host = new Host(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042));
            _resolver = Mock.Of<IEndPointResolver>();
            Mock.Get(_resolver).Setup(resolver => resolver.GetConnectionEndPointAsync(_host, It.IsAny<bool>()))
                .ReturnsAsync((Host h, bool b) => new ConnectionEndPoint(h.Address, null));
            var pool = new HostConnectionPool(
                _host, 
                new TestConfigurationBuilder
                {
                    EndPointResolver = _resolver,
                    ConnectionFactory = new FakeConnectionFactory(),
                    Policies = new Policies(
                        new RoundRobinPolicy(), 
                        new ConstantReconnectionPolicy(1), 
                        new DefaultRetryPolicy(), 
                        NoSpeculativeExecutionPolicy.Instance, 
                        new AtomicMonotonicTimestampGenerator()), 
                    PoolingOptions = PoolingOptions.Create(ProtocolVersion.V4).SetCoreConnectionsPerHost(HostDistance.Local, 2)
                }.Build(), 
                Serializer.Default);
            pool.SetDistance(HostDistance.Local); // set expected connections length
            return pool;
        }
    }
}