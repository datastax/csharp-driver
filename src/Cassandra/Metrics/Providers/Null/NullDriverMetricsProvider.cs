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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Providers.Null
{
    internal class NullDriverMetricsProvider : IDriverMetricsProvider
    {
        public IDriverTimer Timer(string context, string metricName, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit)
        {
            return new NullDriverTimer(metricName);
        }

        public IDriverHistogram Histogram(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new NullDriverHistogram(metricName);
        }

        public IDriverMeter Meter(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new NullDriverMeter(metricName);
        }

        public IDriverCounter Counter(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new NullDriverCounter(metricName);
        }

        public IDriverGauge Gauge(string context, string metricName, Func<double?> valueProvider, DriverMeasurementUnit measurementUnit)
        {
            return new NullDriverGauge(metricName);
        }

        public IDriverGauge Gauge(string context, string metricName, DriverMeasurementUnit measurementUnit)
        {
            return new NullDriverGauge(metricName);
        }

        public void ShutdownMetricsContext(string context)
        {
        }
    }
}