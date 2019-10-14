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
using App.Metrics.Timer;

using Cassandra.AppMetrics.MetricTypes;
using Cassandra.AppMetrics.MetricValues;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc />
    internal class AppMetricsTimer : IAppMetricsTimer
    {
        private readonly IMetrics _appMetrics;
        private readonly ITimer _timer;

        public AppMetricsTimer(
            IMetrics appMetrics, ITimer timer, string bucket, string path)
        {
            _appMetrics = appMetrics;
            _timer = timer;
            Name = path;
            Context = bucket;
        }

        /// <inheritdoc/>
        public void Record(long nanoseconds)
        {
            _timer.Record(nanoseconds, TimeUnit.Nanoseconds);
        }

        /// <inheritdoc/>
        public string Context { get; }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public IAppMetricsTimerValue GetValue()
        {
            var value = _appMetrics.Snapshot.GetForContext(Context).Timers.ValueFor(Name);
            return new AppMetricsTimerValue(value);
        }
    }
}