//
//       Copyright (C) 2019 DataStax Inc.
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

using System.Collections.Generic;
using System.Net;

using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Registries;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Internal
{
    internal class MetricsManager : IMetricsManager
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsManager));

        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly IInternalSession _session;
        private readonly ISessionMetrics _sessionMetricsRegistry;
        private readonly CopyOnWriteDictionary<Host, IMetricsRegistry> _nodeMetricsRegistryCollection;
        private readonly CopyOnWriteDictionary<Host, INodeMetrics> _nodeMetricsCollection;

        public MetricsManager(IDriverMetricsProvider driverMetricsProvider, IInternalSession session)
        {
            _driverMetricsProvider = driverMetricsProvider.WithContext(session.SessionName);
            _session = session;
            _sessionMetricsRegistry = new SessionMetricsRegistry(_session, _driverMetricsProvider);
            _nodeMetricsRegistryCollection = new CopyOnWriteDictionary<Host, IMetricsRegistry>();
            _nodeMetricsCollection = new CopyOnWriteDictionary<Host, INodeMetrics>();
        }

        public IMetricsRegistry SessionMetrics => _sessionMetricsRegistry;

        public IReadOnlyDictionary<Host, IMetricsRegistry> NodeMetrics => _nodeMetricsRegistryCollection;

        public void InitializeMetrics()
        {
            _sessionMetricsRegistry.InitializeMetrics();
        }

        public void RemoveNodeMetrics(Host host)
        {
            _nodeMetricsRegistryCollection.TryRemove(host, out var _);
        }

        public ISessionMetrics GetSessionMetrics()
        {
            return _sessionMetricsRegistry;
        }

        public INodeMetrics GetOrCreateNodeMetrics(Host host)
        {
            if (!_nodeMetricsCollection.TryGetValue(host, out var value))
            {
                var context = MetricsManager.BuildHostAddressMetricPath(host.Address);

                _nodeMetricsRegistryCollection.TryRemove(host, out _);
                _nodeMetricsCollection.TryRemove(host, out _);

                var newRegistry = new NodeMetricsRegistry(_driverMetricsProvider.WithContext("nodes").WithContext(context));
                _nodeMetricsRegistryCollection.Add(host, newRegistry);
                _nodeMetricsCollection.Add(host, newRegistry);

                return newRegistry;
            }

            return value;
        }

        public INodeMetrics AddNodeMetrics(IHostConnectionPool pool)
        {
            var host = pool.Host;
            var context = MetricsManager.BuildHostAddressMetricPath(host.Address);

            _nodeMetricsRegistryCollection.TryRemove(host, out _);
            _nodeMetricsCollection.TryRemove(host, out _);

            var newRegistry = new NodeMetricsRegistry(_driverMetricsProvider.WithContext("nodes").WithContext(context));
            _nodeMetricsRegistryCollection.Add(host, newRegistry);
            _nodeMetricsCollection.Add(host, newRegistry);

            return newRegistry;
        }

        private static string BuildHostAddressMetricPath(IPEndPoint address)
        {
            return $"{address.ToString().Replace('.', '_')}";
        }

        public void Dispose()
        {
            _sessionMetricsRegistry.Dispose();
            foreach (var nodeMetrics in _nodeMetricsCollection.Values)
            {
                nodeMetrics.Dispose();
            }
        }
    }
}