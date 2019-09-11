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

#if NETSTANDARD2_0

using System;
using System.Collections.Generic;
using System.Linq;

using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Histogram;
using App.Metrics.Meter;
using App.Metrics.Timer;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.AppMetrics
{
    internal class AppMetricsDriverMetricsProvider : IDriverMetricsProvider
    {
        private readonly IMetricsRoot _metricsRoot;
        private readonly IEnumerable<string> _contextComponents;

        public AppMetricsDriverMetricsProvider(IMetricsRoot metricsRoot, IEnumerable<string> contextComponents)
        {
            _metricsRoot = metricsRoot;
            _contextComponents = contextComponents;
            CurrentContext = $"{string.Join(".", _contextComponents)}";
        }

        private string CurrentContext { get; }

        public IDriverTimer Timer(string metricName, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit)
        {
            return new AppMetricsTimer(
                _metricsRoot,
                _metricsRoot.Provider.Timer.Instance(new TimerOptions
                {
                    Name = metricName,
                    Context = CurrentContext,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    DurationUnit = timeUnit.ToAppMetricsTimeUnit()
                }),
                CurrentContext,
                metricName);
        }

        public IDriverHistogram Histogram(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverHistogram(
                _metricsRoot,
                _metricsRoot.Provider.Histogram.Instance(new HistogramOptions
                {
                    Name = metricName,
                    Context = CurrentContext,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                CurrentContext,
                metricName);
        }

        public IDriverMeter Meter(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverMeter(
                _metricsRoot,
                _metricsRoot.Provider.Meter.Instance(new MeterOptions
                {
                    Name = metricName,
                    Context = CurrentContext,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                CurrentContext,
                metricName);
        }

        public IDriverCounter Counter(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverCounter(
                _metricsRoot,
                _metricsRoot.Provider.Counter.Instance(new CounterOptions
                {
                    Name = metricName,
                    Context = CurrentContext,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                CurrentContext,
                metricName);
        }

        public IDriverGauge Gauge(string metricName, Func<double> valueProvider, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = CurrentContext,
                        Name = metricName,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build(valueProvider)),
                CurrentContext,
                metricName);
        }

        public IDriverGauge Gauge(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = CurrentContext,
                        Name = metricName,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build()),
                CurrentContext,
                metricName);
        }

        public IDriverMetricsProvider WithContext(string context)
        {
            return new AppMetricsDriverMetricsProvider(
                _metricsRoot, _contextComponents.Concat(new[] { AppMetricsDriverMetricsProvider.FormatContext(context) }));
        }

        private static string FormatContext(string context)
        {
            return context.Replace(".", "_");
        }
    }
}
#endif