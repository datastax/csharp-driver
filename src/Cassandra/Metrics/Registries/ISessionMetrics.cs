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

using System;

using Cassandra.Metrics.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Registries
{
    /// <summary>
    /// Exposes specific session metrics for the driver internals.
    /// </summary>
    internal interface ISessionMetrics : IDisposable
    {
        IDriverTimer CqlRequests { get; }

        IDriverCounter CqlClientTimeouts { get; }

        IDriverMeter BytesSent { get; }

        IDriverMeter BytesReceived { get; }

        IDriverGauge ConnectedNodes { get; }
        
        /// <summary>
        /// Internal MetricsRegistry used to create metrics internally.
        /// </summary>
        IInternalMetricsRegistry<SessionMetric> MetricsRegistry { get; }

        void InitializeMetrics(IInternalSession session);
    }
}