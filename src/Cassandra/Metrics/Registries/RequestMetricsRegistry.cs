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
    internal class RequestMetricsRegistry : IRequestMetrics
    {
        public IDriverMeter Aborted { get; }

        public IDriverMeter ReadTimeout { get; }

        public IDriverMeter WriteTimeout { get; }

        public IDriverMeter Unavailable { get; }

        public IDriverMeter Other { get; }

        public IDriverMeter Total { get; }

        public RequestMetricsRegistry(IDriverMetricsProvider driverMetricsProvider)
        {
            Aborted = driverMetricsProvider.Meter("aborted", DriverMeasurementUnit.Requests);
            ReadTimeout = driverMetricsProvider.Meter("read-timeout", DriverMeasurementUnit.Requests);
            WriteTimeout = driverMetricsProvider.Meter("write-timeout", DriverMeasurementUnit.Requests);
            Unavailable = driverMetricsProvider.Meter("unavailables", DriverMeasurementUnit.Requests);
            Other = driverMetricsProvider.Meter("other", DriverMeasurementUnit.Requests);
            Total = driverMetricsProvider.Meter("total", DriverMeasurementUnit.Requests);
        }

        public IEnumerable<IDriverMeter> Meters => new[] { Aborted, ReadTimeout, WriteTimeout, Unavailable, Other };
    }
}