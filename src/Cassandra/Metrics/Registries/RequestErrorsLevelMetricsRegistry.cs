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

using Cassandra.Metrics.DriverAbstractions;

namespace Cassandra.Metrics.Registries
{
    internal class RequestErrorsLevelMetricsRegistry
    {
        public IDriverMeter Total { get; }
        public IDriverMeter OnAborted { get; }
        public IDriverMeter OnReadTimeout { get; }
        public IDriverMeter OnWriteTimeout { get; }
        public IDriverMeter OnUnavailable { get; }
        public IDriverMeter OnOtherError { get; }

        public RequestErrorsLevelMetricsRegistry(IDriverMetricsProvider driverMetricsProvider)
        {
            Total = driverMetricsProvider.Meter("total", DriverMeasurementUnit.Requests);
            OnAborted = driverMetricsProvider.Meter("aborted", DriverMeasurementUnit.Requests);
            OnReadTimeout = driverMetricsProvider.Meter("read-timeout", DriverMeasurementUnit.Requests);
            OnWriteTimeout = driverMetricsProvider.Meter("write-timeout", DriverMeasurementUnit.Requests);
            OnUnavailable = driverMetricsProvider.Meter("unavailables", DriverMeasurementUnit.Requests);
            OnOtherError = driverMetricsProvider.Meter("other", DriverMeasurementUnit.Requests);
        }
    }
}