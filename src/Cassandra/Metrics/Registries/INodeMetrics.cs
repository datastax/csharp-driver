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

using Cassandra.Connections;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    /// <summary>
    /// Exposes specific node metrics for the driver internals.
    /// </summary>
    internal interface INodeMetrics : IDisposable
    {
        IDriverCounter SpeculativeExecutions { get; }

        IDriverMeter BytesSent { get; }

        IDriverMeter BytesReceived { get; }

        IDriverTimer CqlMessages { get; }

        IDriverGauge OpenConnections { get; }
        
        IDriverGauge InFlight { get; }

        IRequestErrorMetrics Errors { get; }

        IRetryPolicyMetrics Retries { get; }

        IRetryPolicyMetrics Ignores { get; }

        /// <summary>
        /// Internal MetricsRegistry used to create metrics internally.
        /// </summary>
        IInternalMetricsRegistry<NodeMetric> MetricsRegistry { get; }

        /// <summary>
        /// Initialize gauge metrics with a specific connection pool.
        /// </summary>
        /// <param name="pool"></param>
        void InitializePoolGauges(IHostConnectionPool pool);
    }
}