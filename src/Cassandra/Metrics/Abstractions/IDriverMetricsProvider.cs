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

namespace Cassandra.Metrics.Abstractions
{
    public interface IDriverMetricsProvider
    {
        IDriverTimer Timer(string metricName, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit);

        IDriverHistogram Histogram(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverMeter Meter(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverCounter Counter(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverGauge Gauge(string metricName, Func<double?> valueProvider, DriverMeasurementUnit measurementUnit);

        IDriverGauge Gauge(string metricName, DriverMeasurementUnit measurementUnit);

        /// <summary>
        /// Create a new provider with a new context that is the concatenation of the current context plus the provided context.
        /// The current instance should maintain the existing context.
        /// </summary>
        IDriverMetricsProvider WithContext(string context);

        IEnumerable<string> CurrentContext { get; }
    }
}