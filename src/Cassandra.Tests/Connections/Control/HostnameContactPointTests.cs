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

using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections.Control
{
    [TestFixture]
    public class HostnameContactPointTests
    {
        private readonly IPEndPoint _localhostIpEndPoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), HostnameContactPointTests.Port);
        private IDnsResolver _dnsResolver;
        private const int Port = 100;
        
        [Test]
        public async Task Should_EqualsReturnTrue_When_BothContactPointsReferToSameHostname()
        {
            var target = Create("cp1", "127.0.0.1");
            var target2 = Create("cp1", "127.0.0.1");

            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());

            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());
        }

        [Test]
        public async Task Should_EqualsReturnFalse_When_BothContactPointsReferToDifferentHostnames()
        {
            var target = Create("cp1", "127.0.0.1");
            var target2 = Create("cp2", "127.0.0.2");

            Assert.AreNotEqual(target, target2);
            Assert.AreNotEqual(target.GetHashCode(), target2.GetHashCode());

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreNotEqual(target, target2);
            Assert.AreNotEqual(target.GetHashCode(), target2.GetHashCode());

            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreNotEqual(target, target2);
            Assert.AreNotEqual(target.GetHashCode(), target2.GetHashCode());
        }

        [Test]
        public async Task Should_EqualsReturnTrue_When_BothContactPointsReferToSameHostnamesEvenAfterDifferentResolution()
        {
            var target = Create("cp1", "127.0.0.1");
            var target2 = Create("cp1", "127.0.0.1");

            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());

            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());

            Mock.Get(_dnsResolver).Setup(m => m.GetHostEntryAsync("cp1"))
                .ReturnsAsync(new IPHostEntry { AddressList = new[] { IPAddress.Parse("127.0.0.2") }});
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());
            
            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.AreEqual(target, target2);
            Assert.AreEqual(target.GetHashCode(), target2.GetHashCode());
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_DnsResolve_When_HostnameIsProvided(bool refreshCache)
        {
            var target = Create("cp1", "127.0.0.1");
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync("cp1"), Times.Once);
        }
        
        [Test]
        public async Task Should_DnsResolveAgain_When_RefreshCacheIsTrue()
        {
            var target = Create("cp1", "127.0.0.1");
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync("cp1"), Times.Once);
            
            await target.GetConnectionEndPointsAsync(true).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync("cp1"), Times.Exactly(2));
        }
        
        [Test]
        public async Task Should_NotDnsResolveAgain_When_RefreshCacheIsFalse()
        {
            var target = Create("cp1", "127.0.0.1");
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync("cp1"), Times.Once);
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(_dnsResolver).Verify(x => x.GetHostEntryAsync("cp1"), Times.Once);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_HostnameIsProvided()
        {
            var target = Create("cp1", "127.0.0.2");
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"127.0.0.2:{HostnameContactPointTests.Port}", resolved[0].EndpointFriendlyName);
            Assert.AreEqual("cp1", target.StringRepresentation);
            Assert.AreEqual(_localhostIpEndPoint2, resolved[0].GetHostIpEndPointWithFallback());
        }

        [Test]
        public async Task Should_GetCorrectServerName_When_HostnameIsProvided()
        {
            var target = Create("cp1", "127.0.0.2");
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual("127.0.0.2", await resolved[0].GetServerNameAsync().ConfigureAwait(false));
        }
        
        [Test]
        public async Task Should_NotLogMultipleAddressesWarning_When_SingleAddressIsResolved()
        {
            var testListener = new TestTraceListener();
            Trace.Listeners.Add(testListener);
            var level = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            try
            {
                var target = Create("cp1", "127.0.0.2");
                var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

                Assert.AreEqual(1, resolved.Count);
                Assert.AreEqual(0, testListener.Queue.Count);
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = level;
                Trace.Listeners.Remove(testListener);
            }
        }
        
        [Test]
        public async Task Should_LogMultipleAddressesWarning_When_TwoAddressesAreResolved()
        {
            var testListener = new TestTraceListener();
            Trace.Listeners.Add(testListener);
            var level = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            try
            {
                var target = Create("cp1", "127.0.0.2", "127.0.0.3");
                var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

                Assert.AreEqual(2, resolved.Count);
                Assert.AreEqual(1, testListener.Queue.Count(msg => msg.Contains(
                    "Contact point 'cp1' resolved to multiple (2) addresses. " +
                    "Will attempt to use them all if necessary: '127.0.0.2,127.0.0.3'")));
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = level;
                Trace.Listeners.Remove(testListener);
            }
        }

        private HostnameContactPoint Create(string hostname, params string[] ipAddresses)
        {
            _dnsResolver = Mock.Of<IDnsResolver>();
            Mock.Get(_dnsResolver).Setup(m => m.GetHostEntryAsync(hostname))
                .ReturnsAsync(new IPHostEntry { AddressList = ipAddresses.Select(IPAddress.Parse).ToArray() });
            var protocolOptions = new ProtocolOptions(
                HostnameContactPointTests.Port, new SSLOptions().SetHostNameResolver(addr => addr.ToString()));
            var config = new TestConfigurationBuilder
            {
                ProtocolOptions = protocolOptions,
                DnsResolver = _dnsResolver
            }.Build();
            return new HostnameContactPoint(
                config.DnsResolver,
                config.ProtocolOptions,
                config.ServerNameResolver,
                config.KeepContactPointsUnresolved,
                hostname);
        }
    }
}