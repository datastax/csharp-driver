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

namespace Cassandra.Metrics.DriverAbstractions
{
    public interface IDriverMetricsProvider
    {
        IDriverTimer Timer(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverHistogram Histogram(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverMeter Meter(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverCounter Counter(string metricName, DriverMeasurementUnit measurementUnit);

        IDriverGauge Gauge(string metricName, Func<double> instantValue, DriverMeasurementUnit measurementUnit);

        IDriverMetricsProvider WithContext(string context);
    }
}