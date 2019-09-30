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

using System.Collections.Generic;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    internal class RequestErrorMetricsRegistry : IRequestErrorMetrics
    {
        public RequestErrorMetricsRegistry(IDriverMetricsProvider driverMetricsProvider, string context, string metricNamePrefix)
        {
            Aborted = driverMetricsProvider.Meter(context, metricNamePrefix + "request.aborted", DriverMeasurementUnit.Requests);
            ReadTimeout = driverMetricsProvider.Meter(context, metricNamePrefix + "request.read-timeout", DriverMeasurementUnit.Requests);
            WriteTimeout = driverMetricsProvider.Meter(context, metricNamePrefix + "request.write-timeout", DriverMeasurementUnit.Requests);
            Unavailable = driverMetricsProvider.Meter(context, metricNamePrefix + "request.unavailables", DriverMeasurementUnit.Requests);
            Other = driverMetricsProvider.Meter(context, metricNamePrefix + "request.other", DriverMeasurementUnit.Requests);
            Total = driverMetricsProvider.Meter(context, metricNamePrefix + "request.total", DriverMeasurementUnit.Requests);
            Unsent = driverMetricsProvider.Meter(context, metricNamePrefix + "request.unsent", DriverMeasurementUnit.Requests);
            ClientTimeout = driverMetricsProvider.Meter(context, metricNamePrefix + "request.client-timeout", DriverMeasurementUnit.Requests);
            ConnectionInitErrors = driverMetricsProvider.Counter(context, metricNamePrefix + "connection.init", DriverMeasurementUnit.Requests);
            AuthenticationErrors = driverMetricsProvider.Counter(context, metricNamePrefix + "connection.auth", DriverMeasurementUnit.Requests);

            Meters = new[] { Aborted, ReadTimeout, WriteTimeout, Unavailable, Other, Unsent, Total };
            Counters = new[] { ConnectionInitErrors, AuthenticationErrors };
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

        public IEnumerable<IDriverMeter> Meters { get; }

        public IEnumerable<IDriverCounter> Counters { get; }
    }
}