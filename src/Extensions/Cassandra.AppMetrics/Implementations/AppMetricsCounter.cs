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
using App.Metrics.Counter;

using Cassandra.AppMetrics.MetricTypes;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsCounter : IAppMetricsCounter
    {
        private readonly IMetrics _metrics;
        private readonly ICounter _counter;

        public AppMetricsCounter(
            IMetrics metrics, ICounter counter, string bucket, string name)
        {
            _metrics = metrics;
            _counter = counter;
            Context = bucket;
            Name = name;
        }

        /// <inheritdoc />
        public void Increment()
        {
            _counter.Increment();
        }

        /// <inheritdoc />
        public void Increment(long value)
        {
            _counter.Increment(value);
        }

        /// <inheritdoc />
        public string Context { get; }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public long GetValue()
        {
            return _metrics.Snapshot.GetForContext(Context).Counters.ValueFor(Name).Count;
        }
    }
}