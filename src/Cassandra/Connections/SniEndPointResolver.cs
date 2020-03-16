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
    internal class SniEndPointResolver : SingleThreadedResolver, ISniEndPointResolver
    {
        private static readonly Logger Logger = new Logger(typeof(SniEndPointResolver));

        private readonly IDnsResolver _dns;
        private readonly SniOptions _sniOptions;
        private readonly IPEndPoint _endPoint;
        private readonly IRandom _random;
        private volatile IReadOnlyList<IPEndPoint> _resolvedProxyEndPoints;
        private volatile int _index = 0;

        internal SniEndPointResolver(IDnsResolver dns, SniOptions sniOptions, IRandom rand)
        {
            _dns = dns ?? throw new ArgumentNullException(nameof(dns));
            _sniOptions = sniOptions ?? throw new ArgumentNullException(nameof(sniOptions));
            _random = rand ?? throw new ArgumentNullException(nameof(rand));

            if (sniOptions.IsIp)
            {
                _endPoint = new IPEndPoint(sniOptions.Ip, sniOptions.Port);
            }
        }

        public SniEndPointResolver(IDnsResolver dns, SniOptions sniOptions) : this(dns, sniOptions, new DefaultRandom())
        {
        }

        public bool CanBeResolved => true;

        public SniOptions SniOptions => _sniOptions;

        public async Task<IConnectionEndPoint> GetConnectionEndPointAsync(Host host, bool refreshCache)
        {
            return new SniConnectionEndPoint(await GetNextEndPointAsync(refreshCache).ConfigureAwait(false), host.Address, host.HostId.ToString("D"), host.ContactPoint);
        }

        public Task RefreshProxyResolutionAsync()
        {
            return SafeRefreshIfNeededAsync(() => true, UnsafeRefreshProxyResolutionAsync);
        }

        public async Task<IPEndPoint> GetNextEndPointAsync(bool refreshCache)
        {
            if (_endPoint != null)
            {
                return _endPoint;
            }

            await SafeRefreshIfNeededAsync(
                () => refreshCache || _resolvedProxyEndPoints == null, 
                UnsafeRefreshProxyResolutionAsync).ConfigureAwait(false);

            await base.RefreshSemaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                var index = _index;
                index = (index + 1) % _resolvedProxyEndPoints.Count;
                _index = index;
                return _resolvedProxyEndPoints[index];
            }
            finally
            {
                base.RefreshSemaphoreSlim.Release();
            }
        }

        private async Task UnsafeRefreshProxyResolutionAsync()
        {
            if (_endPoint != null)
            {
                return;
            }

            IPHostEntry hostEntry = null;
            Exception exception = null;
            try
            {
                hostEntry = await _dns.GetHostEntryAsync(_sniOptions.Name).ConfigureAwait(false);
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
                                 ? new IPEndPoint(address.MapToIPv4(), _sniOptions.Port)
                                 : new IPEndPoint(address, _sniOptions.Port))
                             .OrderBy(e => _random.Next()) // shuffle
                             .ToList();
                _index = 0;
                return;
            }

            if (_resolvedProxyEndPoints != null)
            {
                var logMsg = 
                    exception == null
                        ? $"DNS resolution of endpoint \"{_sniOptions.Name}\" did not return any host entries. " +
                          "Falling back to the result of the previous DNS resolution."
                        : $"Could not resolve endpoint \"{_sniOptions.Name}\". " +
                          "Falling back to the result of the previous DNS resolution.";

                SniEndPointResolver.Logger.Error(logMsg, exception);
                _index = 0;
                return;
            }
            
            var error = exception == null 
                ? new DriverInternalError($"DNS resolution of endpoint \"{_sniOptions.Name}\" did not return any host entries.")
                : new DriverInternalError($"Could not resolve endpoint \"{_sniOptions.Name}\"", exception);

            throw error;
        }

        private bool TypedEquals(SniEndPointResolver other)
        {
            return object.Equals(_sniOptions, other._sniOptions);
        }

        public bool Equals(ISniEndPointResolver other)
        {
            return Equals((object)other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is SniEndPointResolver typedObj)
            {
                return TypedEquals(typedObj);
            }

            return false;
        }

        public override int GetHashCode()
        {
            return _sniOptions.GetHashCode();
        }
    }
}