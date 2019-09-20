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
using System.Linq;

using Cassandra.Metrics.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    internal class SessionMetricsRegistry : ISessionMetrics
    {
        private readonly IDriverMetricsProvider _driverMetricsProvider;

        public IDriverTimer CqlRequests { get; private set; }
        public IDriverMeter CqlClientTimeouts { get; private set; }
        public IDriverCounter BytesSent { get; private set; }
        public IDriverCounter BytesReceived { get; private set; }
        public IDriverGauge ConnectedNodes { get; private set; }

        public SessionMetricsRegistry(IDriverMetricsProvider driverMetricsProvider)
        {
            _driverMetricsProvider = driverMetricsProvider;
        }

        public void InitializeMetrics(IInternalSession session)
        {
            try
            {
                CqlRequests = _driverMetricsProvider.Timer("cql-requests", DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);
                CqlClientTimeouts = _driverMetricsProvider.Meter("cql-client-timeouts", DriverMeasurementUnit.None);
                BytesSent = _driverMetricsProvider.Counter("bytes-sent", DriverMeasurementUnit.Bytes);
                BytesReceived = _driverMetricsProvider.Counter("bytes-received", DriverMeasurementUnit.Bytes);
                ConnectedNodes = _driverMetricsProvider.Gauge(
                    "connected-nodes",
                    () => session.GetPools().Count(), //TODO
                    DriverMeasurementUnit.None);

                Counters = new[] { BytesSent, BytesReceived };
                Gauges = new[] { ConnectedNodes };
                Meters = new[] { CqlClientTimeouts };
                Timers = new[] { CqlRequests };
            }
            catch (Exception)
            {
                ConnectedNodes?.Dispose();
                throw;
            }
        }

        public IEnumerable<IDriverCounter> Counters { get; private set; }

        public IEnumerable<IDriverGauge> Gauges { get; private set; }

        public IEnumerable<IDriverHistogram> Histograms { get; } = Enumerable.Empty<IDriverHistogram>();

        public IEnumerable<IDriverMeter> Meters { get; private set; }

        public IEnumerable<IDriverTimer> Timers { get; private set; }

        public void Dispose()
        {
            ConnectedNodes?.Dispose();
        }
    }
}