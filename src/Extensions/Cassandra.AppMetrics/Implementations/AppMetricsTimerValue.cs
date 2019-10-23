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
using Cassandra.AppMetrics.MetricValues;

namespace Cassandra.AppMetrics.Implementations
{
    internal class AppMetricsTimerValue : IAppMetricsTimerValue
    {
        public AppMetricsTimerValue(TimerValue value)
        {
            Histogram = new AppMetricsHistogramValue(value.Histogram);
            Rate = new AppMetricsMeterValue(value.Rate);
            DurationUnit = value.DurationUnit;
        }

        public IAppMetricsHistogramValue Histogram { get; }

        public IAppMetricsMeterValue Rate { get; }

        public TimeUnit DurationUnit { get; }
    }
}