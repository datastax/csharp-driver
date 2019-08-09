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
using Cassandra.Metrics.DriverAbstractions;

namespace Cassandra.Metrics.NoopImpl
{
    internal class EmptyDriverMetricsProvider : IDriverMetricsProvider
    {
        public static readonly IDriverMetricsProvider Instance = new EmptyDriverMetricsProvider();

        public IDriverTimer Timer(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return EmptyDriverTimer.Instance;
        }

        public IDriverHistogram Histogram(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return EmptyDriverHistogram.Instance;
        }

        public IDriverMeter Meter(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return EmptyDriverMeter.Instance;
        }

        public IDriverCounter Counter(string metricName, DriverMeasurementUnit measurementUnit)
        {
            return EmptyDriverCounter.Instance;
        }

        public IDriverGauge Gauge(string metricName, Func<double> instantValue, DriverMeasurementUnit measurementUnit)
        {
            return EmptyDriverGauge.Instance;
        }

        public IDriverMetricsProvider WithContext(string context)
        {
            return this;
        }
    }
}