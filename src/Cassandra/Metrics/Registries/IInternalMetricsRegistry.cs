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

namespace Cassandra.Metrics.Registries
{
    /// <summary>
    /// Internal metrics registry that creates metrics (using the configured <see cref="IDriverMetricsProvider"/>).
    /// Also filters the metrics and returns a null implementation if the metric is disabled.
    /// </summary>
    /// <typeparam name="TMetric"></typeparam>
    internal interface IInternalMetricsRegistry<TMetric> : IMetricsRegistry<TMetric> where TMetric : IMetric
    {
        IDriverTimer Timer(string bucket, TMetric metric);
        
        IDriverMeter Meter(string bucket, TMetric metric);

        IDriverCounter Counter(string bucket, TMetric metric);

        IDriverGauge Gauge(string bucket, TMetric metric, Func<double?> valueProvider);

        IDriverMetric GetMetric(TMetric metric);

        /// <summary>
        /// Used to notify the registry that no more metrics will be added. (Concurrency optimization).
        /// </summary>
        void OnMetricsAdded();
    }
}