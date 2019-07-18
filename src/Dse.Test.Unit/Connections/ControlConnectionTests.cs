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
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Dse.Connections;
using Dse.ProtocolEvents;
using Dse.Test.Unit.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.Connections
{
    [TestFixture]
    public class ControlConnectionTests
    {
        private IEndPointResolver _resolver;
        private FakeConnectionFactory _connectionFactory;
        private IPEndPoint _endpoint1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
        private IPEndPoint _endpoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);

        [Test]
        public void Should_ResolveContactPointsAndAttemptEveryOne_When_ContactPointResolutionReturnsMultiple()
        {
            var target = Create();

            var noHostAvailableException = Assert.ThrowsAsync<NoHostAvailableException>(() => target.InitAsync());

            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("cp1"), Times.Once);
            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("127.0.0.1"), Times.Once);
            Mock.Get(_resolver).Verify(r => r.GetOrResolveContactPointAsync("cp2"), Times.Once);
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint1].Count);
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint2].Count);

        }

        private IControlConnection Create()
        {
            _connectionFactory = new FakeConnectionFactory();
            _resolver = Mock.Of<IEndPointResolver>();
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("127.0.0.1")).ReturnsAsync(
                new List<IConnectionEndPoint> { new ConnectionEndPoint(_endpoint1, null) });
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("cp2")).ReturnsAsync(
                new List<IConnectionEndPoint> { new ConnectionEndPoint(_endpoint2, null) });
            Mock.Get(_resolver).Setup(r => r.GetOrResolveContactPointAsync("cp1")).ReturnsAsync(
                new List<IConnectionEndPoint>
                { 
                    new ConnectionEndPoint(_endpoint1, null), 
                    new ConnectionEndPoint(_endpoint2, null)
                });
            var config = new TestConfigurationBuilder
            {
                EndPointResolver = _resolver,
                ConnectionFactory = _connectionFactory
            }.Build();
            return new ControlConnection(
                new ProtocolEventDebouncer(
                    new FakeTimerFactory(), TimeSpan.Zero, TimeSpan.Zero), 
                ProtocolVersion.V3, 
                config, 
                new Metadata(config), 
                new List<object> { "cp1", "cp2", "127.0.0.1" });
        }
    }
}