// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Dse.Connections;
using Dse.Helpers;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.Connections
{
    [TestFixture]
    public class SniEndPointResolverTests
    {
        private readonly IPEndPoint _proxyEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.3"), SniEndPointResolverTests.ProxyPort);
        private readonly IPEndPoint _proxyResolvedEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.4"), SniEndPointResolverTests.ProxyPort);
        private const int Port = 100;
        private const int ProxyPort = 213;

        [Test]
        public async Task Should_NotDnsResolveProxy_When_ProxyIpAddressIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_DnsResolveProxy_When_ProxyNameIsProvided()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ProxyIpAddressIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;

            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_proxyEndPoint, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_proxyEndPoint, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"{_proxyEndPoint} (127.0.0.1)", resolved[0].EndpointFriendlyName);
            Assert.AreEqual(_proxyEndPoint, resolved[0].GetHostIpEndPointWithFallback());
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ProxyNameIsProvided()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;

            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_proxyResolvedEndPoint, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_proxyResolvedEndPoint, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"{_proxyResolvedEndPoint} (127.0.0.1)", resolved[0].EndpointFriendlyName);
            Assert.AreEqual(_proxyResolvedEndPoint, resolved[0].GetHostIpEndPointWithFallback());
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ContactPointNameIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;

            var resolved = (await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual(_proxyEndPoint, resolved[0].GetHostIpEndPointWithFallback());
            Assert.AreEqual(_proxyEndPoint, resolved[0].SocketIpEndPoint);
            Assert.AreEqual($"{_proxyEndPoint} (cp1)", resolved[0].EndpointFriendlyName);
            Assert.AreEqual(_proxyEndPoint, resolved[0].GetHostIpEndPointWithFallback());
        }
        
        [Test]
        public async Task Should_DnsResolveProxy_When_RefreshIsCalled()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.RefreshContactPointCache().ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_ReturnNewResolvedAddress_When_RefreshIsCalled()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var oldResolvedResults = await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false);

            result.ResolveResults[0] = IPAddress.Parse("123.10.10.10");

            await target.RefreshContactPointCache().ConfigureAwait(false);
            var newResolvedResults = await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false);

            Assert.IsFalse(oldResolvedResults.Intersect(newResolvedResults).Any());
        }
        
        [Test]
        public async Task Should_GetCorrectServerName_When_ContactPointIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var resolved = (await target.GetOrResolveContactPointAsync("127.0.0.1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual("127.0.0.1", await resolved[0].GetServerNameAsync().ConfigureAwait(false));
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_NotDnsResolveServerName_When_ContactPointIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var resolved = (await target.GetOrResolveContactPointAsync("cp1").ConfigureAwait(false)).ToList();
            
            Assert.AreEqual(1, resolved.Count);
            Assert.AreEqual("cp1", await resolved[0].GetServerNameAsync().ConfigureAwait(false));
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }

        [Test]
        public async Task Should_NotDnsResolve_When_ResolvingHostButProxyIsIpAddress()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_DnsResolve_When_ResolvingHostAndProxyIsHostname()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHostButProxyIsIpAddress()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Assert.AreEqual(hostId.ToString("D"), await resolved.GetServerNameAsync().ConfigureAwait(false));
            Assert.AreEqual(hostAddress, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(_proxyEndPoint, resolved.SocketIpEndPoint);
            Assert.AreEqual(hostAddress, resolved.GetHostIpEndPointWithFallback());
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHostAndProxyIsHostname()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Assert.AreEqual(hostId.ToString("D"), await resolved.GetServerNameAsync().ConfigureAwait(false));
            Assert.AreEqual(hostAddress, resolved.GetHostIpEndPointWithFallback());
            Assert.AreEqual(_proxyResolvedEndPoint, resolved.SocketIpEndPoint);
            Assert.AreEqual(hostAddress, resolved.GetHostIpEndPointWithFallback());
        }
        
        [Test]
        public async Task Should_CycleThroughResolvedAddresses_When_ResolvingHostAndProxyResolutionReturnsMultipleAddresses()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolvedCollection = new[]
            {
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false)
            };

            async Task AssertResolved(IConnectionEndPoint endPoint, string proxyAddress)
            {
                var proxyEndPoint = new IPEndPoint(IPAddress.Parse(proxyAddress), SniEndPointResolverTests.ProxyPort);
                Assert.AreEqual(hostId.ToString("D"), await endPoint.GetServerNameAsync().ConfigureAwait(false));
                Assert.AreEqual(hostAddress, endPoint.GetHostIpEndPointWithFallback());
                Assert.AreEqual(proxyEndPoint, endPoint.SocketIpEndPoint);
                Assert.AreEqual(hostAddress, endPoint.GetHostIpEndPointWithFallback());
            }

            var resolvedFirst = resolvedCollection.Where(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5").ToList();
            var resolvedSecond = resolvedCollection.Where(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6").ToList();
            Assert.AreEqual(2, resolvedFirst.Count);
            Assert.AreEqual(2, resolvedSecond.Count);
            await AssertResolved(resolvedFirst[0], "127.0.0.5").ConfigureAwait(false);
            await AssertResolved(resolvedFirst[1], "127.0.0.5").ConfigureAwait(false);
            await AssertResolved(resolvedSecond[0], "127.0.0.6").ConfigureAwait(false);
            await AssertResolved(resolvedSecond[1], "127.0.0.6").ConfigureAwait(false);
        }
        
        [Test]
        public async Task Should_NotCallDnsResolve_When_CyclingThroughResolvedProxyAddresses()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_UseNewResolution_When_ResolveSucceeds()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            var listener = new TestTraceListener();
            Trace.Listeners.Add(listener);
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolvedCollection = new List<IConnectionEndPoint>();

            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            
            Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Once);

            await target.RefreshContactPointCache().ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Exactly(2));
            Assert.AreEqual(0, listener.Queue.Count);

            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            
            Assert.AreNotSame(resolvedCollection[0].SocketIpEndPoint, resolvedCollection[2].SocketIpEndPoint);
            Assert.AreNotSame(resolvedCollection[0].SocketIpEndPoint, resolvedCollection[3].SocketIpEndPoint);
            Assert.AreNotSame(resolvedCollection[1].SocketIpEndPoint, resolvedCollection[2].SocketIpEndPoint);
            Assert.AreNotSame(resolvedCollection[1].SocketIpEndPoint, resolvedCollection[3].SocketIpEndPoint);
        }

        [Test]
        public async Task Should_ReusePreviousResolution_When_ResolveFails()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            var listener = new TestTraceListener();
            Trace.Listeners.Add(listener);
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolvedCollection = new List<IConnectionEndPoint>();

            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));

            Assert.AreEqual(0, listener.Queue.Count);
            Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
            Mock.Get(result.DnsResolver).Setup(m => m.GetHostEntryAsync("proxyMultiple")).ThrowsAsync(new Exception());

            await target.RefreshContactPointCache().ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Exactly(2));
            Assert.AreEqual(1, listener.Queue.Count);
            Assert.IsTrue(listener.Queue.ToArray()[0].Contains(
                "Could not resolve endpoint \"proxyMultiple\". Falling back to the result of the previous DNS resolution."));

            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
            
            Assert.AreSame(resolvedCollection[0].SocketIpEndPoint, resolvedCollection[2].SocketIpEndPoint);
            Assert.AreSame(resolvedCollection[1].SocketIpEndPoint, resolvedCollection[3].SocketIpEndPoint);
        }

        [Test]
        public async Task Should_CycleThroughAddressesCorrectly_When_ConcurrentCalls()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolvedCollection = new ConcurrentQueue<IConnectionEndPoint>();
            var tasks = Enumerable.Range(0, 1000)
                .Select(i => Task.Run(async () =>
                {
                    await Task.Delay(1).ConfigureAwait(false);
                    resolvedCollection.Enqueue(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
                }))
                .ToList();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var resolvedArray = resolvedCollection.ToArray();
            var resolvedFirst = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5");
            var resolvedSecond = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6");

            Assert.AreEqual(500, resolvedFirst);
            Assert.AreEqual(500, resolvedSecond);
        }
        
        [Test]
        public async Task Should_CycleThroughAddressesCorrectly_When_ConcurrentCallsWithRefresh()
        {
            var result = Create(name: "proxyMultiple", randValue: 1);
            var target = result.EndPointResolver;
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniEndPointResolverTests.Port);
            var host = new Host(hostAddress);
            var hostId = Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);

            var resolvedCollection = new ConcurrentQueue<IConnectionEndPoint>();

            var tasks = 
                Enumerable.Range(0, 16)
                          .Select(i => Task.Run(async () =>
                          {
                              foreach (var j in Enumerable.Range(0, 10000))
                              {
                                  resolvedCollection.Enqueue(
                                      await target.GetConnectionEndPointAsync(host, (i+j) % 2 == 0).ConfigureAwait(false));
                              }
                          })).ToList();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var resolvedArray = resolvedCollection.ToArray();
            var resolvedFirst = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5");
            var resolvedSecond = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6");

            Assert.AreNotEqual(resolvedFirst, resolvedSecond);
            Assert.AreEqual(160000, resolvedFirst + resolvedSecond);
            Assert.Greater(resolvedFirst, 0);
            Assert.Greater(resolvedSecond, 0);
        }

        private CreateResult Create(IPAddress ip = null, string name = null, int? randValue = null)
        {
            if (ip == null && name == null)
            {
                throw new Exception("ip and name are null");
            }

            if (ip != null && name != null)
            {
                throw new Exception("ip and name are both different than null");
            }

            var dnsResolver = Mock.Of<IDnsResolver>();
            var resolveResults = new[] { IPAddress.Parse("127.0.0.4") };
            var multipleResolveResults = new[] { IPAddress.Parse("127.0.0.5"), IPAddress.Parse("127.0.0.6") };
            Mock.Get(dnsResolver).Setup(m => m.GetHostEntryAsync("proxy"))
                .ReturnsAsync(new IPHostEntry { AddressList = resolveResults });
            Mock.Get(dnsResolver).Setup(m => m.GetHostEntryAsync("proxyMultiple"))
                .ReturnsAsync(new IPHostEntry { AddressList = multipleResolveResults });
            return new CreateResult
            {
                DnsResolver = dnsResolver,
                ResolveResults = resolveResults,
                MultipleResolveResults = multipleResolveResults,
                EndPointResolver = new SniEndPointResolver(dnsResolver, new SniOptions(ip, SniEndPointResolverTests.ProxyPort, name), randValue == null ? (IRandom)new DefaultRandom() : new FixedRandom(randValue.Value))
            };
        }

        private IRow BuildRow(Guid? hostId)
        {
            return new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "host_id", hostId },
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "release_version", "3.11.1" },
                { "tokens", new List<string> { "1" }}
            });
        }

        private class CreateResult
        {
            public IDnsResolver DnsResolver { get; set; }

            public IPAddress[] ResolveResults { get; set; }

            public IPAddress[] MultipleResolveResults { get; set; }

            public IEndPointResolver EndPointResolver { get; set; }
        }
    }
}