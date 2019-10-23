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

using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Meter;
using App.Metrics.Timer;
using Dse.Metrics;
using Dse.Metrics.Abstractions;

namespace Dse.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsDriverMetricsProvider : IDriverMetricsProvider
    {
        private readonly IMetricsRoot _metricsRoot;
        private readonly DriverAppMetricsOptions _options;

        public AppMetricsDriverMetricsProvider(IMetricsRoot appMetrics, DriverAppMetricsOptions options)
        {
            _metricsRoot = appMetrics ?? throw new ArgumentNullException(nameof(appMetrics));
            _options = options;
        }
        
        /// <inheritdoc />
        public IDriverTimer Timer(string bucket, IMetric metric)
        {
            return new AppMetricsTimer(
                _metricsRoot,
                _metricsRoot.Provider.Timer.Instance(new TimerOptions
                {
                    Name = metric.Name,
                    Context = bucket,
                    DurationUnit = _options.TimersTimeUnit,
                    Reservoir = 
                        () => new HdrHistogramReservoir(
                        1, 
                        // timer records value in nanoseconds, convert limit value from ms to ns
                        ((long)_options.HighestLatencyMilliseconds)*1000*1000,
                        _options.SignificantDigits)
                }),
                bucket,
                metric.Name);
        }
        
        /// <inheritdoc />
        public IDriverMeter Meter(string bucket, IMetric metric)
        {
            return new AppMetricsMeter(
                _metricsRoot,
                _metricsRoot.Provider.Meter.Instance(new MeterOptions
                {
                    Name = metric.Name,
                    Context = bucket
                }),
                bucket,
                metric.Name);
        }
        
        /// <inheritdoc />
        public IDriverCounter Counter(string bucket, IMetric metric)
        {
            return new AppMetricsCounter(
                _metricsRoot,
                _metricsRoot.Provider.Counter.Instance(new CounterOptions
                {
                    Name = metric.Name,
                    Context = bucket
                }),
                bucket,
                metric.Name);
        }
        
        /// <inheritdoc />
        public IDriverGauge Gauge(string bucket, IMetric metric, Func<double?> valueProvider)
        {
            return new AppMetricsGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = bucket,
                        Name = metric.Name
                    },
                    () => _metricsRoot.Build.Gauge.Build(() => valueProvider() ?? double.NaN)),
                bucket,
                metric.Name);
        }
        
        /// <inheritdoc />
        public void ShutdownMetricsBucket(string bucket)
        {
            _metricsRoot.Manage.ShutdownContext(bucket);
        }
    }
}