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

namespace Cassandra.Metrics.Abstractions
{
    public interface IDriverMetricsProvider
    {
        IDriverTimer Timer(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit, DriverTimeUnit timeUnit);

        IDriverHistogram Histogram(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit);

        IDriverMeter Meter(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit);

        IDriverCounter Counter(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit);

        IDriverGauge Gauge(string bucket, IMetric metric, Func<double?> valueProvider, DriverMeasurementUnit measurementUnit);

        IDriverGauge Gauge(string bucket, IMetric metric, DriverMeasurementUnit measurementUnit);

        void ShutdownMetricsBucket(string bucket);
    }
}