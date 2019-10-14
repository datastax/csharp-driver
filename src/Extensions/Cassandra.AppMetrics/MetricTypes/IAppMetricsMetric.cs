// 
//       Copyright (C) DataStax Inc.
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

using App.Metrics;
using Cassandra.Metrics;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.MetricTypes
{
    /// <summary>
    /// Common base interface for all metrics of this provider.
    /// </summary>
    public interface IAppMetricsMetric : IDriverMetric
    {
        /// <summary>
        /// Context provided to the AppMetrics library when creating metrics.
        /// This will be set with the bucket name, see any metric creation method
        /// like <see cref="IDriverMetricsProvider.Timer"/> for example.
        /// </summary>
        string Context { get; }

        /// <summary>
        /// Name provided to the AppMetrics library when creating metrics. This will be set with the metric path,
        /// see <see cref="NodeMetric.Path"/> or <see cref="SessionMetric.Path"/>.
        /// </summary>
        string Name { get; }
    }
}