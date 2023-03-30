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

using System.Net;
using System.Threading.Tasks;

using Cassandra.Connections;

using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class EndPointResolverTests
    {
        private const int Port = 100;

        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHost()
        {
            var target = Create();
            var endpoint = new IPEndPoint(IPAddress.Parse("140.20.10.10"), EndPointResolverTests.Port);
            var host = new Host(endpoint, contactPoint: null);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Assert.AreEqual(endpoint, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(endpoint, resolved.SocketIpEndPoint);
            Assert.AreEqual(endpoint, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(endpoint.ToString(), resolved.EndpointFriendlyName);
            Assert.AreEqual("140.20.10.10", await resolved.GetServerNameAsync().ConfigureAwait(false));
        }

        private IEndPointResolver Create()
        {
            var protocolOptions = new ProtocolOptions(
                EndPointResolverTests.Port, new SSLOptions().SetHostNameResolver(addr => addr.ToString()));
            return new EndPointResolver(new ServerNameResolver(protocolOptions));
        }
    }
}