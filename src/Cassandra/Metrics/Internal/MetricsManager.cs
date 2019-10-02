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

using System;
using System.Collections.Generic;
using System.Net;

using Cassandra.Collections;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Registries;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Internal
{
    internal class MetricsManager : IMetricsManager
    {
        private static readonly Logger Logger = new Logger(typeof(MetricsManager));

        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly MetricsOptions _metricsOptions;
        private readonly string _sessionContext;
        private readonly ISessionMetrics _sessionMetrics;
        private readonly CopyOnWriteDictionary<Host, IMetricsRegistry<NodeMetric>> _nodeMetricsRegistryCollection;
        private readonly CopyOnWriteDictionary<Host, INodeMetrics> _nodeMetricsCollection;

        public MetricsManager(IDriverMetricsProvider driverMetricsProvider, MetricsOptions metricsOptions, string sessionName)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _metricsOptions = metricsOptions;
            _sessionContext = metricsOptions.PathPrefix != null ? $"{metricsOptions.PathPrefix}.{sessionName}" : sessionName;
            _sessionMetrics = new SessionMetrics(_driverMetricsProvider, metricsOptions, _sessionContext);
            _nodeMetricsRegistryCollection = new CopyOnWriteDictionary<Host, IMetricsRegistry<NodeMetric>>();
            _nodeMetricsCollection = new CopyOnWriteDictionary<Host, INodeMetrics>();
        }

        public IMetricsRegistry<SessionMetric> SessionMetrics => _sessionMetrics.MetricsRegistry;

        public IReadOnlyDictionary<Host, IMetricsRegistry<NodeMetric>> NodeMetrics => _nodeMetricsRegistryCollection;

        public TMetricType GetNodeMetric<TMetricType>(Host host, NodeMetric nodeMetric) where TMetricType : class, IDriverMetric
        {
            if (!_nodeMetricsCollection.TryGetValue(host, out var nodeMetrics))
            {
                throw new ArgumentException("Could not retrieve metrics for this host: " + host.Address);
            }

            var metric = nodeMetrics.MetricsRegistry.GetMetric(nodeMetric);
            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided metric: ", nodeMetric.Path);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Node Metric {nodeMetric.Path} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        public TMetricType GetSessionMetric<TMetricType>(SessionMetric sessionMetric) where TMetricType : class, IDriverMetric
        {
            var metric = _sessionMetrics.MetricsRegistry.GetMetric(sessionMetric);
            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided session metric: ", sessionMetric.Path);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Session Metric {sessionMetric.Path} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        public void InitializeMetrics(IInternalSession session)
        {
            _sessionMetrics.InitializeMetrics(session);
        }

        public void RemoveNodeMetrics(Host host)
        {
            if (!_nodeMetricsCollection.TryRemove(host, out var nodeMetrics))
            {
                return;
            }

            _nodeMetricsRegistryCollection.TryRemove(host, out _);
            nodeMetrics.Dispose();
        }

        public ISessionMetrics GetSessionMetrics()
        {
            return _sessionMetrics;
        }

        public INodeMetrics GetOrCreateNodeMetrics(Host host)
        {
            var value = _nodeMetricsCollection.GetOrAdd(host, h =>
            {
                var nodeContext = $"{_sessionContext}.nodes.{MetricsManager.BuildHostAddressMetricPath(host.Address)}";

                var newRegistry = new NodeMetrics(_driverMetricsProvider, _metricsOptions, nodeContext);
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