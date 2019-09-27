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
        private readonly string _context;

        public IDriverTimer CqlRequests { get; private set; }
        public IDriverMeter CqlClientTimeouts { get; private set; }
        public IDriverCounter BytesSent { get; private set; }
        public IDriverCounter BytesReceived { get; private set; }
        public IDriverGauge ConnectedNodes { get; private set; }

        public SessionMetricsRegistry(IDriverMetricsProvider driverMetricsProvider, string context)
        {
            _driverMetricsProvider = driverMetricsProvider;
            _context = context;
        }

        public void InitializeMetrics(IInternalSession session)
        {
            try
            {
                CqlRequests = _driverMetricsProvider.Timer(_context, "cql-requests", DriverMeasurementUnit.Requests, DriverTimeUnit.Milliseconds);
                CqlClientTimeouts = _driverMetricsProvider.Meter(_context,"cql-client-timeouts", DriverMeasurementUnit.None);
                BytesSent = _driverMetricsProvider.Counter(_context, "bytes-sent", DriverMeasurementUnit.Bytes);
                BytesReceived = _driverMetricsProvider.Counter(_context, "bytes-received", DriverMeasurementUnit.Bytes);
                ConnectedNodes = _driverMetricsProvider.Gauge(
                    _context,
                    "connected-nodes",
                    () => session.GetPools().Count(), //TODO
                    DriverMeasurementUnit.None);

                Counters = new[] { BytesSent, BytesReceived };
                Gauges = new[] { ConnectedNodes };
                Meters = new[] { CqlClientTimeouts };
                Timers = new[] { CqlRequests };
                All = new IDriverMetric[0].Concat(Counters).Concat(Gauges).Concat(Histograms).Concat(Meters).Concat(Timers);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public IEnumerable<IDriverCounter> Counters { get; private set; }

        public IEnumerable<IDriverGauge> Gauges { get; private set; }

        public IEnumerable<IDriverHistogram> Histograms { get; } = Enumerable.Empty<IDriverHistogram>();

        public IEnumerable<IDriverMeter> Meters { get; private set; }

        public IEnumerable<IDriverTimer> Timers { get; private set; }

        public IEnumerable<IDriverMetric> All { get; private set; }

        public void Dispose()
        {
            _driverMetricsProvider.ShutdownMetricsContext(_context);
        }
    }
}