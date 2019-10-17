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

using Cassandra.Collections;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Registries;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Internal
{
    /// <inheritdoc />
    internal class MetricsManager : IMetricsManager
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsManager));

        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly DriverMetricsOptions _metricsOptions;
        private readonly bool _metricsEnabled;
        private readonly string _sessionBucket;
        private readonly ISessionMetrics _sessionMetrics;
        private readonly CopyOnWriteDictionary<Host, IMetricsRegistry<NodeMetric>> _nodeMetricsRegistryCollection;
        private readonly CopyOnWriteDictionary<Host, INodeMetrics> _nodeMetricsCollection;
        private readonly bool _disabledSessionTimerMetrics;
        private readonly bool _disabledNodeTimerMetrics;

        public MetricsManager(IDriverMetricsProvider driverMetricsProvider, DriverMetricsOptions metricsOptions, bool metricsEnabled, string sessionName)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _metricsOptions = metricsOptions;
            _metricsEnabled = metricsEnabled;
            _sessionBucket = metricsOptions.BucketPrefix != null ? $"{metricsOptions.BucketPrefix}.{sessionName}" : sessionName;
            _sessionMetrics = new SessionMetrics(_driverMetricsProvider, metricsOptions, metricsEnabled, _sessionBucket);
            _nodeMetricsRegistryCollection = new CopyOnWriteDictionary<Host, IMetricsRegistry<NodeMetric>>();
            _nodeMetricsCollection = new CopyOnWriteDictionary<Host, INodeMetrics>();
            _disabledSessionTimerMetrics = !metricsEnabled || !metricsOptions.EnabledSessionMetrics.Contains(SessionMetric.Timers.CqlRequests);
            _disabledNodeTimerMetrics = !metricsEnabled || !metricsOptions.EnabledNodeMetrics.Contains(NodeMetric.Timers.CqlMessages);
        }

        /// <inheritdoc/>
        public IMetricsRegistry<SessionMetric> SessionMetrics => _sessionMetrics.MetricsRegistry;
        
        /// <inheritdoc/>
        public IReadOnlyDictionary<Host, IMetricsRegistry<NodeMetric>> NodeMetrics => _nodeMetricsRegistryCollection;
        
        /// <inheritdoc/>
        public bool AreMetricsEnabled => _metricsEnabled;

        /// <inheritdoc />
        public TMetricType GetNodeMetric<TMetricType>(Host host, NodeMetric nodeMetric) where TMetricType : class, IDriverMetric
        {
            if (!_nodeMetricsCollection.TryGetValue(host, out var nodeMetrics))
            {
                throw new ArgumentException("Could not retrieve metrics for this host: " + host.Address);
            }

            var metric = nodeMetrics.MetricsRegistry.GetMetric(nodeMetric);
            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided metric: ", nodeMetric.Name);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Node Metric {nodeMetric.Name} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        /// <inheritdoc />
        public TMetricType GetSessionMetric<TMetricType>(SessionMetric sessionMetric) where TMetricType : class, IDriverMetric
        {
            var metric = _sessionMetrics.MetricsRegistry.GetMetric(sessionMetric);
            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided session metric: ", sessionMetric.Name);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Session Metric {sessionMetric.Name} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        /// <inheritdoc />
        public void InitializeMetrics(IInternalSession session)
        {
            _sessionMetrics.InitializeMetrics(session);
        }

        /// <inheritdoc />
        public void RemoveNodeMetrics(Host host)
        {
            if (!_nodeMetricsCollection.TryRemove(host, out var nodeMetrics))
            {
                return;
            }

            _nodeMetricsRegistryCollection.TryRemove(host, out _);
            nodeMetrics.Dispose();
        }

        /// <inheritdoc />
        public bool AreSessionTimerMetricsEnabled => !_disabledSessionTimerMetrics;
        
        /// <inheritdoc />
        public bool AreNodeTimerMetricsEnabled => !_disabledNodeTimerMetrics;
        
        /// <inheritdoc />
        public ISessionMetrics GetSessionMetrics()
        {
            return _sessionMetrics;
        }

        /// <inheritdoc />
        public INodeMetrics GetOrCreateNodeMetrics(Host host)
        {
            var value = _nodeMetricsCollection.GetOrAdd(host, h =>
            {
                var nodeBucket = $"{_sessionBucket}.nodes.{MetricsManager.BuildHostAddressMetricPath(host.Address)}";

                var newRegistry = new NodeMetrics(_driverMetricsProvider, _metricsOptions, _metricsEnabled, nodeBucket);
                _nodeMetricsRegistryCollection.Add(host, newRegistry.MetricsRegistry);

                return newRegistry;
            });

            return value;
        }

        private static string BuildHostAddressMetricPath(IPEndPoint address)
        {
            return $"{address.ToString().Replace('.', '_')}";
        }

        public void Dispose()
        {
            _sessionMetrics.Dispose();
            foreach (var nodeMetrics in _nodeMetricsCollection.Values)
            {
                nodeMetrics.Dispose();
            }
        }
    }
}