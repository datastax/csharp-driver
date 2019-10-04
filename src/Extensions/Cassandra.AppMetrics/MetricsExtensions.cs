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
            MetricsExtensions.ThrowIfNull(driverMetrics, host, nodeMetric);
            return driverMetrics.GetNodeMetric<IAppMetricsCounter>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based gauge type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsGauge GetNodeGauge(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, host, nodeMetric);
            return driverMetrics.GetNodeMetric<IAppMetricsGauge>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based meter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsMeter GetNodeMeter(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, host, nodeMetric);
            return driverMetrics.GetNodeMetric<IAppMetricsMeter>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/> with the appropriate AppMetrics based timer type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetNodeMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsTimer GetNodeTimer(this IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, host, nodeMetric);
            return driverMetrics.GetNodeMetric<IAppMetricsTimer>(host, nodeMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based counter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsCounter GetSessionCounter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, sessionMetric);
            return driverMetrics.GetSessionMetric<IAppMetricsCounter>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based gauge type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsGauge GetSessionGauge(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, sessionMetric);
            return driverMetrics.GetSessionMetric<IAppMetricsGauge>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based meter type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsMeter GetSessionMeter(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, sessionMetric);
            return driverMetrics.GetSessionMetric<IAppMetricsMeter>(sessionMetric);
        }
        
        /// <summary>
        /// Utility method that wraps a call to <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/> with the appropriate AppMetrics based timer type
        /// as the type parameter. For more information see the API docs of <see cref="IDriverMetrics.GetSessionMetric{TMetricType}"/>.
        /// </summary>
        public static IAppMetricsTimer GetSessionTimer(this IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, sessionMetric);
            return driverMetrics.GetSessionMetric<IAppMetricsTimer>(sessionMetric);
        }

        /// <summary>
        /// Casts the provided counter to the counter implementation of this provider.
        /// </summary>
        /// <exception cref="ArgumentException">If the counter was not created by the AppMetrics
        /// provider (<see cref="CreateDriverMetricsProvider"/>).</exception>
        public static IAppMetricsCounter ToAppMetricsCounter(this IDriverCounter counter)
        {
            MetricsExtensions.ThrowIfNull(counter, nameof(counter));

            if (counter is IAppMetricsCounter appMetricsCounter)
            {
                return appMetricsCounter;
            }

            throw new ArgumentException("Counter was not created by the AppMetricsDriverProvider, " +
                                        $"it's type is {counter.GetType().Name} and doesn't implement IAppMetricsCounter.");
        }
        
        /// <summary>
        /// Casts the provided gauge to the gauge implementation of this provider.
        /// </summary>
        /// <exception cref="ArgumentException">If the gauge was not created by the AppMetrics
        /// provider (<see cref="CreateDriverMetricsProvider"/>).</exception>
        public static IAppMetricsGauge ToAppMetricsGauge(this IDriverGauge gauge)
        {
            MetricsExtensions.ThrowIfNull(gauge, nameof(gauge));

            if (gauge is IAppMetricsGauge appMetricsGauge)
            {
                return appMetricsGauge;
            }

            throw new ArgumentException("Gauge was not created by the AppMetricsDriverProvider, " +
                                        $"it's type is {gauge.GetType().Name} and doesn't implement IAppMetricsGauge.");
        }
        
        /// <summary>
        /// Casts the provided meter to the meter implementation of this provider.
        /// </summary>
        /// <exception cref="ArgumentException">If the meter was not created by the AppMetrics
        /// provider (<see cref="CreateDriverMetricsProvider"/>).</exception>
        public static IAppMetricsMeter ToAppMetricsMeter(this IDriverMeter meter)
        {
            MetricsExtensions.ThrowIfNull(meter, nameof(meter));

            if (meter is IAppMetricsMeter appMetricsMeter)
            {
                return appMetricsMeter;
            }

            throw new ArgumentException("Counter was not created by the AppMetricsDriverProvider, " +
                                        $"it's type is {meter.GetType().Name} and doesn't implement IAppMetricsMeter.");
        }

        /// <summary>
        /// Casts the provided timer to the timer implementation of this provider.
        /// </summary>
        /// <exception cref="ArgumentException">If the timer was not created by the AppMetrics
        /// provider (<see cref="CreateDriverMetricsProvider"/>).</exception>
        public static IAppMetricsTimer ToAppMetricsTimer(this IDriverTimer timer)
        {
            MetricsExtensions.ThrowIfNull(timer, nameof(timer));

            if (timer is IAppMetricsTimer appMetricsTimer)
            {
                return appMetricsTimer;
            }

            throw new ArgumentException("Timer was not created by the AppMetricsDriverProvider, " +
                                        $"it's type is {timer.GetType().Name} and doesn't implement IAppMetricsTimer.");
        }

        private static void ThrowIfNull(object obj, string name)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(name);
            }
        }

        private static void ThrowIfNull(IDriverMetrics driverMetrics, SessionMetric sessionMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, nameof(driverMetrics));
            MetricsExtensions.ThrowIfNull(sessionMetric, nameof(sessionMetric));
        }
        
        private static void ThrowIfNull(IDriverMetrics driverMetrics, Host host, NodeMetric nodeMetric)
        {
            MetricsExtensions.ThrowIfNull(driverMetrics, nameof(driverMetrics));
            MetricsExtensions.ThrowIfNull(host, nameof(host));
            MetricsExtensions.ThrowIfNull(nodeMetric, nameof(nodeMetric));
        }
    }
}