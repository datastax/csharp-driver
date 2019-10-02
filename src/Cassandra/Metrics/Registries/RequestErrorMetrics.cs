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
    internal class RequestErrorMetrics : IRequestErrorMetrics
    {
        public RequestErrorMetrics(IInternalMetricsRegistry<NodeMetric> nodeMetricsRegistry, string context)
        {
            Aborted = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.AbortedRequests, DriverMeasurementUnit.Requests);
            ReadTimeout = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.ReadTimeouts, DriverMeasurementUnit.Requests);
            WriteTimeout = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.WriteTimeouts, DriverMeasurementUnit.Requests);
            Unavailable = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.UnavailableErrors, DriverMeasurementUnit.Requests);
            Other = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.OtherErrors, DriverMeasurementUnit.Requests);
            Total = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.Errors, DriverMeasurementUnit.Requests);
            Unsent = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.UnsentRequests, DriverMeasurementUnit.Requests);
            ClientTimeout = nodeMetricsRegistry.Meter(context, NodeMetric.Meters.ClientTimeouts, DriverMeasurementUnit.Requests);
            ConnectionInitErrors = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.ConnectionInitErrors, DriverMeasurementUnit.Requests);
            AuthenticationErrors = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.AuthenticationErrors, DriverMeasurementUnit.Requests);
        }

        public IDriverMeter Aborted { get; }

        public IDriverMeter ReadTimeout { get; }

        public IDriverMeter WriteTimeout { get; }

        public IDriverMeter Unavailable { get; }

        public IDriverMeter ClientTimeout { get; }

        public IDriverMeter Other { get; }

        public IDriverMeter Unsent { get; }

        public IDriverCounter ConnectionInitErrors { get; }

        public IDriverCounter AuthenticationErrors { get; }

        public IDriverMeter Total { get; }
    }
}