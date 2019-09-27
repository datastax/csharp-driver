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
        private readonly string _sessionContext;
        private readonly ISessionMetrics _sessionMetricsRegistry;
        private readonly CopyOnWriteDictionary<Host, IMetricsRegistry> _nodeMetricsRegistryCollection;
        private readonly CopyOnWriteDictionary<Host, INodeMetrics> _nodeMetricsCollection;

        private readonly IReadOnlyDictionary<NodeMetric, Func<INodeMetrics, IDriverMetric>> _nodeMetricsGetters;
        private readonly IReadOnlyDictionary<SessionMetric, Func<ISessionMetrics, IDriverMetric>> _sessionMetricsGetters;

        public MetricsManager(IDriverMetricsProvider driverMetricsProvider, string customContext, string sessionName)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _sessionContext = customContext != null ? $"{customContext}.{sessionName}" : sessionName;
            _sessionMetricsRegistry = new SessionMetricsRegistry(_driverMetricsProvider, _sessionContext);
            _nodeMetricsRegistryCollection = new CopyOnWriteDictionary<Host, IMetricsRegistry>();
            _nodeMetricsCollection = new CopyOnWriteDictionary<Host, INodeMetrics>();

            _nodeMetricsGetters = new Dictionary<NodeMetric, Func<INodeMetrics, IDriverMetric>>
            {
                { Cassandra.NodeMetrics.Meters.Errors.Request.Total, metrics => metrics.Errors.Total },
                { Cassandra.NodeMetrics.Meters.Errors.Request.Aborted, metrics => metrics.Errors.Aborted },
                { Cassandra.NodeMetrics.Meters.Errors.Request.Other, metrics => metrics.Errors.Other },
                { Cassandra.NodeMetrics.Meters.Errors.Request.ReadTimeout, metrics => metrics.Errors.ReadTimeout },
                { Cassandra.NodeMetrics.Meters.Errors.Request.Unavailable, metrics => metrics.Errors.Unavailable },
                { Cassandra.NodeMetrics.Meters.Errors.Request.Unsent, metrics => metrics.Errors.Unavailable }, // TODO
                { Cassandra.NodeMetrics.Meters.Errors.Request.WriteTimeout, metrics => metrics.Errors.WriteTimeout },

                { Cassandra.NodeMetrics.Meters.Ignores.Total, metrics => metrics.Ignores.Total },
                { Cassandra.NodeMetrics.Meters.Ignores.Aborted, metrics => metrics.Ignores.Aborted },
                { Cassandra.NodeMetrics.Meters.Ignores.Other, metrics => metrics.Ignores.Other },
                { Cassandra.NodeMetrics.Meters.Ignores.ReadTimeout, metrics => metrics.Ignores.ReadTimeout },
                { Cassandra.NodeMetrics.Meters.Ignores.Unavailable, metrics => metrics.Ignores.Unavailable },
                { Cassandra.NodeMetrics.Meters.Ignores.WriteTimeout, metrics => metrics.Ignores.WriteTimeout },

                { Cassandra.NodeMetrics.Meters.Retries.Total, metrics => metrics.Retries.Total },
                { Cassandra.NodeMetrics.Meters.Retries.Aborted, metrics => metrics.Retries.Aborted },
                { Cassandra.NodeMetrics.Meters.Retries.Other, metrics => metrics.Retries.Other },
                { Cassandra.NodeMetrics.Meters.Retries.ReadTimeout, metrics => metrics.Retries.ReadTimeout },
                { Cassandra.NodeMetrics.Meters.Retries.Unavailable, metrics => metrics.Retries.Unavailable },
                { Cassandra.NodeMetrics.Meters.Retries.WriteTimeout, metrics => metrics.Retries.WriteTimeout },
                
                { Cassandra.NodeMetrics.Counters.BytesReceived, metrics => metrics.BytesReceived },
                { Cassandra.NodeMetrics.Counters.BytesSent, metrics => metrics.BytesSent },
                { Cassandra.NodeMetrics.Counters.SpeculativeExecutions, metrics => metrics.SpeculativeExecutions },
                
                { Cassandra.NodeMetrics.Gauges.Pool.OpenConnections, metrics => metrics.OpenConnections },
                { Cassandra.NodeMetrics.Gauges.Pool.AvailableStreams, metrics => metrics.AvailableStreams },
                { Cassandra.NodeMetrics.Gauges.Pool.InFlight, metrics => metrics.InFlight },
                { Cassandra.NodeMetrics.Gauges.Pool.MaxRequestsPerConnection, metrics => metrics.MaxRequestsPerConnection },
                
                { Cassandra.NodeMetrics.Timers.CqlMessages, metrics => metrics.CqlMessages }
            };
            
            _sessionMetricsGetters = new Dictionary<SessionMetric, Func<ISessionMetrics, IDriverMetric>>
            {
                { Cassandra.SessionMetrics.Meters.CqlClientTimeouts, metrics => metrics.CqlClientTimeouts },
                { Cassandra.SessionMetrics.Counters.BytesSent, metrics => metrics.BytesSent },
                { Cassandra.SessionMetrics.Counters.BytesReceived, metrics => metrics.BytesReceived },
                { Cassandra.SessionMetrics.Gauges.ConnectedNodes, metrics => metrics.BytesReceived }, // TODO
                { Cassandra.SessionMetrics.Timers.CqlRequests, metrics => metrics.CqlRequests },
            };
        }

        public IMetricsRegistry SessionMetrics => _sessionMetricsRegistry;

        public IReadOnlyDictionary<Host, IMetricsRegistry> NodeMetrics => _nodeMetricsRegistryCollection;

        public TMetricType GetNodeMetric<TMetricType>(Host host, NodeMetric nodeMetric) where TMetricType : class, IDriverMetric
        {
            if (!_nodeMetricsCollection.TryGetValue(host, out var nodeMetrics))
            {
                throw new ArgumentException("Could not retrieve metrics for this host: " + host.Address);
            }
            
            _nodeMetricsGetters.TryGetValue(nodeMetric, out var getter);
            var metric = getter?.Invoke(nodeMetrics);

            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided metric: ", nodeMetric.Name);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Node Metric {metric.FullName} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        public TMetricType GetSessionMetric<TMetricType>(SessionMetric sessionMetric) where TMetricType : class, IDriverMetric
        {
            _sessionMetricsGetters.TryGetValue(sessionMetric, out var getter);
            var metric = getter?.Invoke(_sessionMetricsRegistry);

            if (metric == null)
            {
                throw new ArgumentException("Could not find the provided session metric: ", sessionMetric.Name);
            }

            if (!(metric is TMetricType typedMetric))
            {
                throw new ArgumentException(
                    $"Session Metric {metric.FullName} is not of type {typeof(TMetricType).Name}. Its type is {metric.GetType().Name}.");
            }

            return typedMetric;
        }

        public void InitializeMetrics(IInternalSession session)
        {
            _sessionMetricsRegistry.InitializeMetrics(session);
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
            return _sessionMetricsRegistry;
        }

        public INodeMetrics GetOrCreateNodeMetrics(Host host)
        {
            var value = _nodeMetricsCollection.GetOrAdd(host, h =>
            {
                var nodeContext = $"{_sessionContext}.nodes.{MetricsManager.BuildHostAddressMetricPath(host.Address)}";

                var newRegistry = new NodeMetricsRegistry(_driverMetricsProvider, nodeContext);
                _nodeMetricsRegistryCollection.Add(host, newRegistry);

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
            _sessionMetricsRegistry.Dispose();
            foreach (var nodeMetrics in _nodeMetricsCollection.Values)
            {
                nodeMetrics.Dispose();
            }
        }
    }
}