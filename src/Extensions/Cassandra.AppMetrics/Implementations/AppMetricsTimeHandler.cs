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
using System.Diagnostics;
using App.Metrics;
using App.Metrics.Timer;

using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    /// <inheritdoc cref="IDriverTimerMeasurement" />
    internal class AppMetricsTimerMeasurement : IDriverTimerMeasurement
    {
        private static readonly long Factor = 1000L * 1000L * 1000L / Stopwatch.Frequency;

        private readonly ITimer _timer;
        private readonly long _start;

        public AppMetricsTimerMeasurement(ITimer timer, long timestamp)
        {
            _timer = timer;
            _start = timestamp;
        }

        /// <inheritdoc/>
        public void StopMeasuring(long timestamp)
        {
            _timer.Record((timestamp - _start) * Factor, TimeUnit.Nanoseconds);
        }
    }
}