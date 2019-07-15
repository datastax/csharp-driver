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
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class EndPointResolverTests
    {
        private readonly IPEndPoint _localhostIpEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), EndPointResolverTests.Port);
        private readonly IPEndPoint _localhostIpEndPoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), EndPointResolverTests.Port);
        private IDnsResolver _dnsResolver;
        private const int Port = 100;

        [Test]
        public async Task Should_NotDnsResolve_When_IpAddressIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_NotDnsResolve_When_ResolvingHost()
        {
            var target = Create();
            var endpoint = new IPEndPoint(IPAddress.Parse("140.20.10.10"), EndPointResolverTests.Port);
            var host = new Host(endpoint);
            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_IpAddressIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_localhostIpEndPoint, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_localhostIpEndPoint, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"127.0.0.1:{EndPointResolverTests.Port}", resolved[0].EndpointFriendlyName);
            Assert.AreEqual(_localhostIpEndPoint, resolved[0].GetHostIpEndPointWithFallback());
        }

        [Test]
        public async Task Should_GetCorrectServerName_When_IpAddressIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual("127.0.0.1", await resolved[0].GetServerNameAsync().ConfigureAwait(false));
        }
        
        [Test]
        public async Task Should_DnsResolve_When_HostnameIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false)).ToList();

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_HostnameIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"127.0.0.2:{EndPointResolverTests.Port}", resolved[0].EndpointFriendlyName);
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].GetHostIpEndPointWithFallback());
        }

        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHost()
        {
            var target = Create();
            var endpoint = new IPEndPoint(IPAddress.Parse("140.20.10.10"), EndPointResolverTests.Port);
            var host = new Host(endpoint);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Assert.AreEqual(endpoint, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(endpoint, resolved.SocketIpEndPoint);
            Assert.AreEqual(endpoint, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(endpoint.ToString(), resolved.EndpointFriendlyName);
            Assert.AreEqual("140.20.10.10", await resolved.GetServerNameAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task Should_GetCorrectServerName_When_HostnameIsProvided()
        {
            var target = Create();
            var resolved = (await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual("127.0.0.2", await resolved[0].GetServerNameAsync().ConfigureAwait(false));
        }

        private IEndPointResolver Create()
        {
            _dnsResolver = Mock.Of<IDnsResolver>();
            Mock.Get(_dnsResolver).Setup(m => m.GetHostEntryAsync("cp1"))
                .ReturnsAsync(new IPHostEntry { AddressList = new[] { IPAddress.Parse("127.0.0.2") }});
            var protocolOptions = new ProtocolOptions(
                EndPointResolverTests.Port, new SSLOptions().SetHostNameResolver(addr => addr.ToString()));
            return new EndPointResolver(_dnsResolver, protocolOptions);
        }
    }
}