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

using System.Collections.Generic;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics
{
    /// <summary>
    /// Metrics Registry.
    /// </summary>
    /// <typeparam name="TMetric">Should be <see cref="NodeMetric"/> out <see cref="SessionMetric"/>.</typeparam>
    public interface IMetricsRegistry<TMetric> where TMetric : IMetric
    {
        /// <summary>
        /// Dictionary with counter metrics.
        /// </summary>
        IReadOnlyDictionary<TMetric, IDriverCounter> Counters { get; }
        
        /// <summary>
        /// Dictionary with gauge metrics.
        /// </summary>
        IReadOnlyDictionary<TMetric, IDriverGauge> Gauges { get; }
        
        /// <summary>
        /// Dictionary with meter metrics.
        /// </summary>
        IReadOnlyDictionary<TMetric, IDriverMeter> Meters { get; }

        /// <summary>
        /// Dictionary with timer metrics.
        /// </summary>
        IReadOnlyDictionary<TMetric, IDriverTimer> Timers { get; }

        /// <summary>
        /// Dictionary with metrics of all types. The values can be cast to the appropriate type interface
        /// (or the implementation that is provider specific).
        /// </summary>
        IReadOnlyDictionary<TMetric, IDriverMetric> Metrics { get; }
    }
}