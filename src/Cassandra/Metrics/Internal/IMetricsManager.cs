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

using Cassandra.Metrics.Registries;
using Cassandra.SessionManagement;

namespace Cassandra.Metrics.Internal
{
    /// <summary>
    /// Implements <see cref="IDriverMetrics"/> and exposes methods for the driver internals to update metrics.
    /// </summary>
    internal interface IMetricsManager : IDriverMetrics, IDisposable
    {
        ISessionMetrics GetSessionMetrics();

        /// <summary>
        /// Get the existing node metrics for the provided host or creates them and returns them if they don't exist yet.
        /// </summary>
        INodeMetrics GetOrCreateNodeMetrics(Host host);

        /// <summary>
        /// Initialize metrics with the provided session.
        /// </summary>
        void InitializeMetrics(IInternalSession session);

        /// <summary>
        /// 
        /// </summary>
        void RemoveNodeMetrics(Host host);

        /// <summary>
        /// Whether SessionMetrics of type Timer are enabled.
        /// </summary>
        bool AreSessionTimerMetricsEnabled { get; }
        
        /// <summary>
        /// Whether NodeMetrics of type Timer are enabled.
        /// </summary>
        bool AreNodeTimerMetricsEnabled { get; }

        /// <summary>
        /// Whether metrics are enabled.
        /// </summary>
        bool AreMetricsEnabled { get; }
    }
}