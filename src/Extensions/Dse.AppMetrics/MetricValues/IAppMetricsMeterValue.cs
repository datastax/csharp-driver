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
using App.Metrics.Meter;

namespace Dse.AppMetrics.MetricValues
{
    /// <summary>
    /// Meter value based on <see cref="MeterValue"/>.
    /// </summary>
    public interface IAppMetricsMeterValue
    {
        /// <summary>
        /// Count
        /// </summary>
        long Count { get; }

        /// <summary>
        /// Rate per 15 minutes.
        /// </summary>
        double FifteenMinuteRate { get; }

        /// <summary>
        /// Rate per 5 minutes.
        /// </summary>
        double FiveMinuteRate { get; }

        /// <summary>
        /// Mean rate.
        /// </summary>
        double MeanRate { get; }

        /// <summary>
        /// Rate per minute.
        /// </summary>
        double OneMinuteRate { get; }

        /// <summary>
        /// Time unit for this meter value.
        /// </summary>
        TimeUnit RateUnit { get; }
    }
}