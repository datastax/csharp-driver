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
using App.Metrics.Timer;

namespace Dse.AppMetrics.MetricValues
{
    /// <summary>
    /// Timer value based on <see cref="TimerValue"/>.
    /// </summary>
    public interface IAppMetricsTimerValue
    {
        /// <summary>
        /// Histogram value for this timer metric.
        /// </summary>
        IAppMetricsHistogramValue Histogram { get; }

        /// <summary>
        /// Meter value for this timer metric.
        /// </summary>
        IAppMetricsMeterValue Rate { get; }

        /// <summary>
        /// This is obtained from <see cref="DriverAppMetricsOptions.TimersTimeUnit"/>.
        /// </summary>
        TimeUnit DurationUnit { get; }
    }
}