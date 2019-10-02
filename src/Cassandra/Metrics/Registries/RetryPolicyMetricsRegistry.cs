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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    internal class RetryPolicyOnRetryMetrics : IRetryPolicyMetrics
    {
        public RetryPolicyOnRetryMetrics(IInternalMetricsRegistry<NodeMetric> nodeMetricsRegistry, string context)
        {
            ReadTimeout = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.RetriesOnReadTimeout, DriverMeasurementUnit.Requests);
            WriteTimeout = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.RetriesOnWriteTimeout, DriverMeasurementUnit.Requests);
            Unavailable = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.RetriesOnUnavailable, DriverMeasurementUnit.Requests);
            Other = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.RetriesOnOtherError, DriverMeasurementUnit.Requests);
            Total = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.Retries, DriverMeasurementUnit.Requests);
        }

        public IDriverMeter ReadTimeout { get; }

        public IDriverMeter WriteTimeout { get; }

        public IDriverMeter Unavailable { get; }

        public IDriverMeter Other { get; }

        public IDriverMeter Total { get; }
    }
}