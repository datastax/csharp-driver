//
//       Copyright DataStax, Inc.
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

using Cassandra.Requests;
using Cassandra.Serialization;
using Moq;

using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class RequestExecutionTests
    {
        [Test]
        public void Should_ThrowException_WhenNoValidHosts()
        {
            var mockSession = Mock.Of<IInternalSession>();
            var mockRequest = Mock.Of<IRequest>();
            var mockRequestExecution = Mock.Of<IRequestHandler>();
            Mock.Get(mockRequestExecution)
                .Setup(m => m.GetNextValidHost(It.IsAny<Dictionary<IPEndPoint, Exception>>()))
                .Throws(new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>()));
            var sut = new ProxyRequestExecution(mockRequestExecution, mockSession, mockRequest);

            Assert.Throws<NoHostAvailableException>(() => sut.Start(false));
        }
        
        [Test]
        public void Should_NotThrowException_WhenAValidHostIsObtained()
        {
            var mockSession = Mock.Of<IInternalSession>();
            var mockRequest = Mock.Of<IRequest>();
            var mockRequestExecution = Mock.Of<IRequestHandler>();
            var host = new Host(
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9047),
                new ConstantReconnectionPolicy(1));
            host.BringUpIfDown();
            var validHost = ValidHost.New(
                host,
                HostDistance.Local);
            Mock.Get(mockRequestExecution)
                .SetupSequence(m => m.GetNextValidHost(It.IsAny<Dictionary<IPEndPoint, Exception>>()))
                .Returns(validHost)
                .Returns(null);
            var sut = new ProxyRequestExecution(mockRequestExecution, mockSession, mockRequest);

            sut.Start(false);
        }
        
        private class ProxyRequestExecution : RequestExecution
        {
            public ProxyRequestExecution(IRequestHandler parent, IInternalSession session, IRequest request) : base(parent, session, request)
            {
            }

            protected override IRequestHandler NewRequestHandler(IInternalSession session, Serializer serializer, IRequest request, IStatement statement)
            {
                return Mock.Of<IRequestHandler>();
            }
        }
    }
}