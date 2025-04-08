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
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using Cassandra.Helpers;

namespace Cassandra.Connections
{
    internal class SniEndPointResolver : ISniEndPointResolver, IEquatable<SniEndPointResolver>
    {
        private static readonly Logger Logger = new Logger(typeof(SniEndPointResolver));

        private readonly IDnsResolver _dns;
        private readonly IRandom _random;
        private readonly SingleThreadedResolver _proxyDnsResolver;
        private readonly ISniOptionsProvider _sniOptionsProvider;

        private volatile IReadOnlyList<IPEndPoint> _resolvedProxyEndPoints;
        private volatile int _index = 0;
        private volatile IPEndPoint _endPoint;

        internal SniEndPointResolver(
            ISniOptionsProvider sniOptionsProvider,
            IDnsResolver dns,
            IRandom rand)
        {
            _sniOptionsProvider = sniOptionsProvider ?? throw new ArgumentNullException(nameof(sniOptionsProvider));
            _dns = dns ?? throw new ArgumentNullException(nameof(dns));
            _random = rand ?? throw new ArgumentNullException(nameof(rand));
            _proxyDnsResolver = new SingleThreadedResolver();
        }

        public SniEndPointResolver(
            ISniOptionsProvider sniOptionsProvider,
            IDnsResolver dns) :
            this(sniOptionsProvider, dns, new DefaultRandom())
        {
        }

        public async Task<IConnectionEndPoint> GetConnectionShardAwareEndPointAsync(Host host, bool refreshCache, int shardID, int shardAwarePort)
        {
            return new SniConnectionEndPoint(
                await GetNextEndPointAsync(refreshCache).ConfigureAwait(false),
                new IPEndPoint(IPAddress.Parse(host.Address.ToString().Split(':')[0]), shardAwarePort),
                host.HostId.ToString("D"),
                host.ContactPoint);
        }

        public async Task<IConnectionEndPoint> GetConnectionEndPointAsync(Host host, bool refreshCache)
        {
            return new SniConnectionEndPoint(
                await GetNextEndPointAsync(refreshCache).ConfigureAwait(false),
                host.Address,
                host.HostId.ToString("D"),
                host.ContactPoint);
        }

        public async Task<SniOptions> RefreshAsync(bool refreshSniOptions, bool refreshCache)
        {
            var sniOptions = await _sniOptionsProvider.GetAsync(refreshSniOptions).ConfigureAwait(false);
            await _proxyDnsResolver.RefreshIfNeededAsync(
                () => refreshSniOptions || refreshCache || (_resolvedProxyEndPoints == null && _endPoint == null),
                () => UnsafeRefreshProxyEndpointsAsync(sniOptions)).ConfigureAwait(false);
            return sniOptions;
        }

        public async Task<IPEndPoint> GetNextEndPointAsync(bool refreshCache)
        {
            if (_endPoint != null)
            {
                return _endPoint;
            }

            await RefreshAsync(!_sniOptionsProvider.IsInitialized(), refreshCache).ConfigureAwait(false);

            // double check because it might not have been initialized before refresh
            if (_endPoint != null)
            {
                return _endPoint;
            }

            await _proxyDnsResolver.RefreshSemaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                var index = _index;
                index = (index + 1) % _resolvedProxyEndPoints.Count;
                _index = index;
                return _resolvedProxyEndPoints[index];
            }
            finally
            {
                _proxyDnsResolver.RefreshSemaphoreSlim.Release();
            }
        }

        public Task<SniOptions> GetSniOptionsAsync(bool refreshSniOptions)
        {
            return RefreshAsync(refreshSniOptions, refreshSniOptions);
        }

        public string GetStaticIdentifier()
        {
            return _sniOptionsProvider.GetStaticIdentifier();
        }

        private async Task UnsafeRefreshProxyEndpointsAsync(SniOptions sniOptions)
        {
            if (sniOptions.IsIp && _endPoint == null)
            {
                _endPoint = new IPEndPoint(sniOptions.Ip, sniOptions.Port);
            }

            if (_endPoint != null)
            {
                return;
            }

            if (sniOptions == null)
            {
                throw new DriverInternalError("sniOptions is null when refreshing proxy endpoints, this is most likely a driver bug");
            }

            IPHostEntry hostEntry = null;
            Exception exception = null;
            try
            {
                hostEntry = await _dns.GetHostEntryAsync(sniOptions.Name).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            if (hostEntry != null && hostEntry.AddressList.Length > 0)
            {
                _resolvedProxyEndPoints =
                    hostEntry.AddressList
                             .Where(address => address.AddressFamily != AddressFamily.InterNetworkV6 || address.IsIPv4MappedToIPv6)
                             .Select(address => address.IsIPv4MappedToIPv6
                                 ? new IPEndPoint(address.MapToIPv4(), sniOptions.Port)
                                 : new IPEndPoint(address, sniOptions.Port))
                             .OrderBy(e => _random.Next()) // shuffle
                             .ToList();
                _index = 0;
                return;
            }

            if (_resolvedProxyEndPoints != null)
            {
                var logMsg =
                    exception == null
                        ? $"DNS resolution of endpoint \"{sniOptions.Name}\" did not return any host entries. " +
                          "Falling back to the result of the previous DNS resolution."
                        : $"Could not resolve endpoint \"{sniOptions.Name}\". " +
                          "Falling back to the result of the previous DNS resolution.";

                SniEndPointResolver.Logger.Error(logMsg, exception);
                _index = 0;
                return;
            }

            var error = exception == null
                ? new DriverInternalError($"DNS resolution of endpoint \"{sniOptions.Name}\" did not return any host entries.")
                : new DriverInternalError($"Could not resolve endpoint \"{sniOptions.Name}\"", exception);

            throw error;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as SniEndPointResolver);
        }

        public bool Equals(SniEndPointResolver other)
        {
            return other != null &&
                   EqualityComparer<ISniOptionsProvider>.Default.Equals(_sniOptionsProvider, other._sniOptionsProvider);
        }

        public override int GetHashCode()
        {
            return -1739232371 + EqualityComparer<ISniOptionsProvider>.Default.GetHashCode(_sniOptionsProvider);
        }

        public static bool operator ==(SniEndPointResolver left, SniEndPointResolver right)
        {
            return EqualityComparer<SniEndPointResolver>.Default.Equals(left, right);
        }

        public static bool operator !=(SniEndPointResolver left, SniEndPointResolver right)
        {
            return !(left == right);
        }
    }
}