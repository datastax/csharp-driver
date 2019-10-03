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

using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Histogram;
using App.Metrics.Meter;
using App.Metrics.Timer;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsDriverMetricsProvider : IDriverMetricsProvider
    {
        private readonly IMetricsRoot _metricsRoot;

        public AppMetricsDriverMetricsProvider(IMetricsRoot appMetrics)
        {
            _metricsRoot = appMetrics ?? throw new ArgumentNullException(nameof(appMetrics));
        }
        
        /// <inheritdoc />
        public IDriverTimer Timer(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit)
        {
            return new AppMetricsTimer(
                _metricsRoot,
                _metricsRoot.Provider.Timer.Instance(new TimerOptions
                {
                    Name = metric.Path,
                    Context = bucket,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    DurationUnit = timeUnit.ToAppMetricsTimeUnit()
                }),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public IDriverHistogram Histogram(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsHistogram(
                _metricsRoot,
                _metricsRoot.Provider.Histogram.Instance(new HistogramOptions
                {
                    Name = metric.Path,
                    Context = bucket,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public IDriverMeter Meter(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsMeter(
                _metricsRoot,
                _metricsRoot.Provider.Meter.Instance(new MeterOptions
                {
                    Name = metric.Path,
                    Context = bucket,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public IDriverCounter Counter(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsCounter(
                _metricsRoot,
                _metricsRoot.Provider.Counter.Instance(new CounterOptions
                {
                    Name = metric.Path,
                    Context = bucket,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit()
                }),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public IDriverGauge Gauge(string bucket, IMetric metric, Func<double?> valueProvider, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = bucket,
                        Name = metric.Path,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build(() => valueProvider() ?? double.NaN)),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public IDriverGauge Gauge(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = bucket,
                        Name = metric.Path,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build()),
                bucket,
                metric.Path,
                measurementUnit);
        }
        
        /// <inheritdoc />
        public void ShutdownMetricsBucket(string bucket)
        {
            _metricsRoot.Manage.ShutdownContext(bucket);
        }
    }
}