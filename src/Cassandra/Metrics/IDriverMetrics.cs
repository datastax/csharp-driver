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
using System.Collections.Generic;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics
{
    /// <summary>
    /// Exposes driver metrics.
    /// </summary>
    public interface IDriverMetrics
    {
        /// <summary>
        /// Exposes session metrics for the session from which this instance was retrieved. See <see cref="ISession.GetMetrics"/>.
        /// </summary>
        IMetricsRegistry<SessionMetric> SessionMetrics { get; }
        
        /// <summary>
        /// Exposes node metrics for the hosts used in requests executed by the session
        /// from which this instance was retrieved. See <see cref="ISession.GetMetrics"/>.
        /// </summary>
        IReadOnlyDictionary<Host, IMetricsRegistry<NodeMetric>> NodeMetrics { get; }

        /// <summary>
        /// Gets a specific node metric of a specific host. <typeparamref name="TMetricType"/> can be any type in the
        /// inheritance tree of the metric object returned by the <see cref="IDriverMetricsProvider"/>.
        /// </summary>
        /// <exception cref="ArgumentException">This exception is thrown if the metric object can not be cast to <typeparamref name="TMetricType"/>.</exception>
        TMetricType GetNodeMetric<TMetricType>(Host host, NodeMetric nodeMetric) where TMetricType : class, IDriverMetric;
        
        /// <summary>
        /// Gets a specific session metric. <typeparamref name="TMetricType"/> can be any type in the
        /// inheritance tree of the metric object returned by the <see cref="IDriverMetricsProvider"/>.
        /// </summary>
        /// <exception cref="ArgumentException">This exception is thrown if the metric object can not be cast to <typeparamref name="TMetricType"/>.</exception>
        TMetricType GetSessionMetric<TMetricType>(SessionMetric sessionMetric) where TMetricType : class, IDriverMetric;
    }
}