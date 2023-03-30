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
using System.Net;
using System.Threading.Tasks;

using Cassandra.Connections;

namespace Cassandra.DataStax.Cloud
{
    internal class CloudSniOptionsProvider : ISniOptionsProvider, IEquatable<CloudSniOptionsProvider>
    {
        private readonly SecureConnectionBundle _bundle;
        private readonly SSLOptions _sslOptions;
        private readonly SocketOptions _socketOptions;
        private readonly ICloudMetadataService _cloudMetadataService;
        private readonly SingleThreadedResolver _cloudMetadataResolver;

        private volatile CloudMetadataResult _cloudMetadataResult;
        private volatile SniOptions _sniOptions;

        public CloudSniOptionsProvider(
            SocketOptions socketOptions,
            SSLOptions sslOptions,
            SecureConnectionBundle bundle,
            ICloudMetadataService cloudMetadataService)
        {
            _socketOptions = socketOptions ?? throw new ArgumentNullException(nameof(socketOptions));
            _sslOptions = sslOptions ?? throw new ArgumentNullException(nameof(sslOptions));
            _bundle = bundle ?? throw new ArgumentNullException(nameof(bundle));
            _cloudMetadataService = cloudMetadataService ?? throw new ArgumentNullException(nameof(cloudMetadataService));

            _cloudMetadataResolver = new SingleThreadedResolver();
        }

        private async Task UnsafeRefreshSniOptions()
        {
            var cloudMetadata = await _cloudMetadataService.GetClusterMetadataAsync(
                $"https://{_bundle.Config.Host}:{_bundle.Config.Port}/metadata",
                _socketOptions,
                _sslOptions).ConfigureAwait(false);

            var proxyAddress = cloudMetadata.ContactInfo.SniProxyAddress;
            var separatorIndex = proxyAddress.IndexOf(':');

            if (separatorIndex == -1)
            {
                throw new InvalidOperationException($"The SNI endpoint address should contain ip/name and port. Address received: {proxyAddress}");
            }

            if (cloudMetadata.ContactInfo.ContactPoints == null || cloudMetadata.ContactInfo.ContactPoints.Count == 0)
            {
                throw new InvalidOperationException("The metadata service returned 0 contact points.");
            }

            var ipOrName = proxyAddress.Substring(0, separatorIndex);
            var port = int.Parse(proxyAddress.Substring(separatorIndex + 1));
            var isIp = IPAddress.TryParse(ipOrName, out var addr);
            var sniOptions = new SniOptions(addr, port, isIp ? null : ipOrName, new SortedSet<string>(cloudMetadata.ContactInfo.ContactPoints));

            _sniOptions = sniOptions;
            _cloudMetadataResult = cloudMetadata;
        }

        public async Task<CloudMetadataResult> InitializeAsync()
        {
            await _cloudMetadataResolver.RefreshIfNeededAsync(() => true, UnsafeRefreshSniOptions).ConfigureAwait(false);
            return _cloudMetadataResult;
        }

        public async Task<SniOptions> GetAsync(bool refresh)
        {
            if (refresh || !IsInitialized())
            {
                await _cloudMetadataResolver.RefreshIfNeededAsync(() => true, UnsafeRefreshSniOptions).ConfigureAwait(false);
            }

            return _sniOptions;
        }

        public bool IsInitialized()
        {
            return _sniOptions != null && _cloudMetadataResult != null;
        }

        public string GetStaticIdentifier()
        {
            return $"{_bundle.Config.Host}:{_bundle.Config.Port}";
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as CloudSniOptionsProvider);
        }

        public bool Equals(CloudSniOptionsProvider other)
        {
            return other != null &&
                   EqualityComparer<SecureConnectionBundle>.Default.Equals(_bundle, other._bundle);
        }

        public override int GetHashCode()
        {
            return -631637154 + EqualityComparer<SecureConnectionBundle>.Default.GetHashCode(_bundle);
        }

        public static bool operator ==(CloudSniOptionsProvider left, CloudSniOptionsProvider right)
        {
            return EqualityComparer<CloudSniOptionsProvider>.Default.Equals(left, right);
        }

        public static bool operator !=(CloudSniOptionsProvider left, CloudSniOptionsProvider right)
        {
            return !(left == right);
        }
    }
}