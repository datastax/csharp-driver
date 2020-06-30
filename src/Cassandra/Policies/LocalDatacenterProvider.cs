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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Connections.Control;
using Cassandra.SessionManagement;

namespace Cassandra
{
    internal class LocalDatacenterProvider : ILocalDatacenterProvider
    {
        private volatile bool _initialized = false;

        private volatile IInternalCluster _cluster;
        private volatile IInternalMetadata _internalMetadata;
        private volatile IEnumerable<string> _availableDcs;
        private volatile string _availableDcsStr;
        private volatile string _cachedDatacenter;

        public void Initialize(IInternalCluster cluster, IInternalMetadata internalMetadata)
        {
            _cluster = cluster;
            _internalMetadata = internalMetadata;
            _availableDcs = internalMetadata.AllHosts().Select(h => h.Datacenter).Where(dc => dc != null).Distinct().ToList();
            _availableDcsStr = string.Join(", ", _availableDcs);
            _initialized = true;
        }

        public string DiscoverLocalDatacenter(bool inferLocalDc, string policyDatacenter)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("This object was not initialized.");
            }

            if (!string.IsNullOrEmpty(policyDatacenter))
            {
                return ValidateAndReturnDatacenter(policyDatacenter);
            }

            if (!string.IsNullOrEmpty(_cachedDatacenter))
            {
                return _cachedDatacenter;
            }

            if (!string.IsNullOrEmpty(_cluster.Configuration.LocalDatacenter))
            {
                _cachedDatacenter = ValidateAndReturnDatacenter(_cluster.Configuration.LocalDatacenter);
                return _cachedDatacenter;
            }

            if (!_cluster.ImplicitContactPoint && !inferLocalDc)
            {
                throw new InvalidOperationException(
                    "Since you provided explicit contact points, the local datacenter must be explicitly set. " +
                    "It can be specified in the load balancing policy constructor or " +
                    $"via the Builder.WithLocalDatacenter() method. Available datacenters: {_availableDcsStr}.");
            }

            // implicit contact point so infer the local datacenter from the control connection host
            return InferLocalDatacenter();
        }

        private string ValidateAndReturnDatacenter(string datacenter)
        {
            //Check that the datacenter exists
            if (!_availableDcs.Contains(datacenter))
            {
                throw new ArgumentException(
                    $"Datacenter {datacenter} does not match any of the nodes, available datacenters: {_availableDcsStr}.");
            }

            return datacenter;
        }

        private string InferLocalDatacenter()
        {
            var cc = _internalMetadata.ControlConnection;
            if (cc == null)
            {
                throw new DriverInternalError("ControlConnection was not correctly set");
            }

            // Use the host used by the control connection
            _cachedDatacenter =
                cc.Host?.Datacenter ??
                throw new InvalidOperationException(
                    "The local datacenter could not be inferred from the implicit contact point, " +
                    "please set it explicitly in the load balancing policy constructor or " +
                    $"via the Builder.WithLocalDatacenter() method. Available datacenters: {_availableDcsStr}.");

            return _cachedDatacenter;
        }
    }
}