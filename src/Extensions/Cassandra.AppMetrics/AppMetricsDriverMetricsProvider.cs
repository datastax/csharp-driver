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
using System.Linq;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Histogram;
using App.Metrics.Meter;
using App.Metrics.Timer;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics
{
    public class AppMetricsDriverMetricsProvider : IDriverMetricsProvider
    {
        private readonly IMetricsRoot _metricsRoot;

        public AppMetricsDriverMetricsProvider(IMetricsRoot metricsRoot)
        {
            _metricsRoot = metricsRoot;
        }
        
        public IDriverTimer Timer(string context, string metricName, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit)
        {
            return new AppMetricsTimer(
                _metricsRoot,
                _metricsRoot.Provider.Timer.Instance(new TimerOptions
                {
                    Name = metricName,
                    Context = context,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    DurationUnit = timeUnit.ToAppMetricsTimeUnit()
                }),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        public IDriverHistogram Histogram(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverHistogram(
                _metricsRoot,
                _metricsRoot.Provider.Histogram.Instance(new HistogramOptions
                {
                    Name = metricName,
                    Context = context,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        public IDriverMeter Meter(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverMeter(
                _metricsRoot,
                _metricsRoot.Provider.Meter.Instance(new MeterOptions
                {
                    Name = metricName,
                    Context = context,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                }),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        public IDriverCounter Counter(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverCounter(
                _metricsRoot,
                _metricsRoot.Provider.Counter.Instance(new CounterOptions
                {
                    Name = metricName,
                    Context = context,
                    MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    ReportItemPercentages = false,
                    ReportSetItems = false,
                }),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        public IDriverGauge Gauge(string context, string metricName, Func<double?> valueProvider, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = context,
                        Name = metricName,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build(() => valueProvider() ?? double.NaN)),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        public IDriverGauge Gauge(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new AppMetricsDriverGauge(
                _metricsRoot,
                _metricsRoot.Provider.Gauge.Instance(
                    new GaugeOptions
                    {
                        Context = context,
                        Name = metricName,
                        MeasurementUnit = measurementUnit.ToAppMetricsUnit(),
                    },
                    () => _metricsRoot.Build.Gauge.Build()),
                context,
                metricName,
                ComputeFullMetricName(context, metricName));
        }

        private string ComputeFullMetricName(string context, string name)
        {
            return $"{context}.{name}";
        }

        public void ShutdownMetricsContext(string context)
        {
            _metricsRoot.Manage.ShutdownContext(context);
        }
    }
}