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

using App.Metrics;

using Cassandra.AppMetrics.Implementations;
using Cassandra.AppMetrics.MetricTypes;
using Cassandra.Metrics;
using Cassandra.Metrics.Abstractions;

namespace Cassandra
{
    public static class MetricsExtensions
    {
        /// <summary>
        /// Creates a <see cref="IDriverMetricsProvider"/> based on AppMetrics with the provided <see cref="IMetricsRoot"/>.
        /// </summary>
        public static IDriverMetricsProvider CreateDriverMetricsProvider(this IMetricsRoot appMetrics)
        {
            return new AppMetricsDriverMetricsProvider(appMetrics);
        }

        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based counter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsCounter GetNodeCounter(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            return driverMetrics.GetNodeMetric<IAppMetricsCounter>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based gauge type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsGauge GetNodeGauge(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            return driverMetrics.GetNodeMetric<IAppMetricsGauge>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based histogram type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsHistogram GetNodeHistogram(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            return driverMetrics.GetNodeMetric<IAppMetricsHistogram>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based meter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsMeter GetNodeMeter(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            return driverMetrics.GetNodeMetric<IAppMetricsMeter>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based timer type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsTimer GetNodeTimer(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            return driverMetrics.GetNodeMetric<IAppMetricsTimer>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based counter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsCounter GetSessionCounter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            return driverMetrics.GetSessionMetric<IAppMetricsCounter>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based gauge type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsGauge GetSessionGauge(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            return driverMetrics.GetSessionMetric<IAppMetricsGauge>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based histogram type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsHistogram GetSessionHistogram(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            return driverMetrics.GetSessionMetric<IAppMetricsHistogram>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based meter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsMeter GetSessionMeter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            return driverMetrics.GetSessionMetric<IAppMetricsMeter>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based timer type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsTimer GetSessionTimer(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            return driverMetrics.GetSessionMetric<IAppMetricsTimer>(sessionMetric);
        }
    }
}