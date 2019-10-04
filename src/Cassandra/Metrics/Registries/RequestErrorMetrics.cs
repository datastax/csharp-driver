//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    internal class RequestErrorMetrics : IRequestErrorMetrics
    {
        public RequestErrorMetrics(IInternalMetricsRegistry<NodeMetric> nodeMetricsRegistry, string context)
        {
            Aborted = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.AbortedRequests, DriverMeasurementUnit.Requests);
            ReadTimeout = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.ReadTimeouts, DriverMeasurementUnit.Requests);
            WriteTimeout = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.WriteTimeouts, DriverMeasurementUnit.Requests);
            Unavailable = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.UnavailableErrors, DriverMeasurementUnit.Requests);
            Other = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.OtherErrors, DriverMeasurementUnit.Requests);
            Total = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.Errors, DriverMeasurementUnit.Requests);
            Unsent = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.UnsentRequests, DriverMeasurementUnit.Requests);
            ClientTimeout = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.ClientTimeouts, DriverMeasurementUnit.Requests);
            ConnectionInitErrors = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.ConnectionInitErrors, DriverMeasurementUnit.Requests);
            AuthenticationErrors = nodeMetricsRegistry.Counter(context, NodeMetric.Counters.AuthenticationErrors, DriverMeasurementUnit.Requests);
        }

        public IDriverCounter Aborted { get; }

        public IDriverCounter ReadTimeout { get; }

        public IDriverCounter WriteTimeout { get; }

        public IDriverCounter Unavailable { get; }

        public IDriverCounter ClientTimeout { get; }

        public IDriverCounter Other { get; }

        public IDriverCounter Unsent { get; }

        public IDriverCounter ConnectionInitErrors { get; }

        public IDriverCounter AuthenticationErrors { get; }

        public IDriverCounter Total { get; }
    }
}