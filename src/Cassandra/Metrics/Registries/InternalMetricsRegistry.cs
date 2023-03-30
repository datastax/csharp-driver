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
using System.Threading;

using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Providers.Null;

namespace Cassandra.Metrics.Registries
{
    /// <inheritdoc />
    internal class InternalMetricsRegistry<TMetric> : IInternalMetricsRegistry<TMetric> where TMetric : IMetric
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;
        private readonly bool _metricsEnabled;
        private readonly HashSet<TMetric> _disabledMetrics;

        private readonly Dictionary<TMetric, IDriverGauge> _gauges = new Dictionary<TMetric, IDriverGauge>();
        private readonly Dictionary<TMetric, IDriverCounter> _counters = new Dictionary<TMetric, IDriverCounter>();
        private readonly Dictionary<TMetric, IDriverMeter> _meters = new Dictionary<TMetric, IDriverMeter>();
        private readonly Dictionary<TMetric, IDriverTimer> _timers = new Dictionary<TMetric, IDriverTimer>();
        private readonly Dictionary<TMetric, IDriverMetric> _metrics = new Dictionary<TMetric, IDriverMetric>();

        private bool _initialized = false;

        public InternalMetricsRegistry(IDriverMetricsProvider driverMetricsProvider, IEnumerable<TMetric> disabledMetrics, bool metricsEnabled)
        {
            _disabledMetrics = new HashSet<TMetric>(disabledMetrics);
            _driverMetricsProvider = driverMetricsProvider;
            _metricsEnabled = metricsEnabled;
        }

        /// <inheritdoc />
        public IReadOnlyDictionary<TMetric, IDriverCounter> Counters => _counters;

        /// <inheritdoc />
        public IReadOnlyDictionary<TMetric, IDriverGauge> Gauges => _gauges;
        
        /// <inheritdoc />
        public IReadOnlyDictionary<TMetric, IDriverMeter> Meters => _meters;

        /// <inheritdoc />
        public IReadOnlyDictionary<TMetric, IDriverTimer> Timers => _timers;

        /// <inheritdoc />
        public IReadOnlyDictionary<TMetric, IDriverMetric> Metrics => _metrics;

        public IDriverTimer Timer(string bucket, TMetric metric)
        {
            ThrowIfInitialized();
            if (!IsMetricEnabled(metric))
            {
                return NullDriverTimer.Instance;
            }

            var timer = _driverMetricsProvider.Timer(bucket, metric);
            _timers.Add(metric, timer);
            _metrics.Add(metric, timer);
            return timer;
        }
        
        public IDriverMeter Meter(string bucket, TMetric metric)
        {
            ThrowIfInitialized();
            if (!IsMetricEnabled(metric))
            {
                return NullDriverMeter.Instance;
            }

            var meter = _driverMetricsProvider.Meter(bucket, metric);
            _meters.Add(metric, meter);
            _metrics.Add(metric, meter);
            return meter;
        }

        public IDriverCounter Counter(string bucket, TMetric metric)
        {
            ThrowIfInitialized();
            if (!IsMetricEnabled(metric))
            {
                return NullDriverCounter.Instance;
            }

            var counter = _driverMetricsProvider.Counter(bucket, metric);
            _counters.Add(metric, counter);
            _metrics.Add(metric, counter);
            return counter;
        }

        public IDriverGauge Gauge(string bucket, TMetric metric, Func<double?> valueProvider)
        {
            ThrowIfInitialized();
            if (!IsMetricEnabled(metric))
            {
                return NullDriverGauge.Instance;
            }

            var gauge = _driverMetricsProvider.Gauge(bucket, metric, valueProvider);
            _gauges.Add(metric, gauge);
            _metrics.Add(metric, gauge);
            return gauge;
        }

        public IDriverMetric GetMetric(TMetric metric)
        {
            _metrics.TryGetValue(metric, out var driverMetric);
            return driverMetric;
        }

        /// <inheritdoc />
        public void OnMetricsAdded()
        {
            _initialized = true;
            Interlocked.MemoryBarrier();
        }

        private void ThrowIfInitialized()
        {
            if (_initialized)
            {
                throw new DriverInternalError("Can not add metrics after initialization is complete.");
            }
        }

        private bool IsMetricEnabled(TMetric metric)
        {
            return _metricsEnabled && !_disabledMetrics.Contains(metric);
        }
    }
}