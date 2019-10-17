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

using App.Metrics;
using App.Metrics.Meter;

using Cassandra.AppMetrics.MetricTypes;
using Cassandra.AppMetrics.MetricValues;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsMeter : IAppMetricsMeter
    {
        private readonly IMetrics _metrics;
        private readonly IMeter _meter;

        public AppMetricsMeter(
            IMetrics metrics, IMeter meter, string bucket, string name)
        {
            _metrics = metrics;
            _meter = meter;
            Context = bucket;
            Name = name;
        }

        /// <inheritdoc />
        public void Mark(long amount)
        {
            _meter.Mark(amount);
        }

        /// <inheritdoc />
        public string Context { get; }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public IAppMetricsMeterValue GetValue()
        {
            return new AppMetricsMeterValue(_metrics.Snapshot.GetForContext(Context).Meters.ValueFor(Name));
        }
    }
}