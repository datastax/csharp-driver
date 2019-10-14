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
using System.Collections.Generic;
using System.Linq;
using App.Metrics;
using Cassandra.Metrics.Abstractions;

namespace Cassandra.AppMetrics.Implementations
{
    internal static class AppMetricsExtensions
    {
        public static DriverTimeUnit ToDriverTimeUnit(this TimeUnit timeUnit)
        {
            switch (timeUnit)
            {
                case TimeUnit.Days:
                    return DriverTimeUnit.Days;

                case TimeUnit.Hours:
                    return DriverTimeUnit.Hours;

                case TimeUnit.Microseconds:
                    return DriverTimeUnit.Microseconds;

                case TimeUnit.Milliseconds:
                    return DriverTimeUnit.Milliseconds;

                case TimeUnit.Minutes:
                    return DriverTimeUnit.Minutes;

                case TimeUnit.Nanoseconds:
                    return DriverTimeUnit.Nanoseconds;

                case TimeUnit.Seconds:
                    return DriverTimeUnit.Seconds;

                default:
                    throw new ArgumentOutOfRangeException(nameof(timeUnit), timeUnit, null);
            }
        }

        public static T ValueFor<T>(this IEnumerable<MetricValueSourceBase<T>> values, string metricName)
        {
            var selectedValues = values.Where(t => t.Name == metricName).Select(t => t.Value).Take(2).ToList();

            return selectedValues.Count == 1 ? selectedValues.First() : default(T);
        }
    }
}