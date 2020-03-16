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

using System;
using App.Metrics;

namespace Cassandra.AppMetrics
{
    /// <summary>
    /// Options related to the app metrics provider.
    /// Currently the existing options are used to configure the HdrHistogram used internally for Timer metrics.
    /// </summary>
    public class DriverAppMetricsOptions
    {
        /// <summary>
        /// See <see cref="SetHighestLatencyMilliseconds"/> for information about this property.
        /// </summary>
        public int HighestLatencyMilliseconds { get; private set; } = 30000;
        
        /// <summary>
        /// See <see cref="SetSignificantDigits"/> for information about this property.
        /// </summary>
        public int SignificantDigits { get; private set; } = 3;

        /// <summary>
        /// See <see cref="SetTimersTimeUnit"/> for information about this property.
        /// </summary>
        public TimeUnit TimersTimeUnit { get; private set; } = TimeUnit.Nanoseconds;
        
        /// <summary>
        /// See <see cref="SetRefreshIntervalMilliseconds"/> for information about this property.
        /// </summary>
        public int RefreshIntervalMilliseconds { get; private set; } = 10000;

        /// <summary>
        /// <para>
        /// The largest latency that we expect to record.
        /// </para>
        /// <para>
        /// This should be slightly higher than <see cref="SocketOptions.ReadTimeoutMillis"/> and <see cref="Builder.WithQueryTimeout"/>
        /// (in theory, readings can't be higher than the timeout, but there might be a small overhead due to internal scheduling).
        /// </para> 
        /// <para>
        /// This is used to scale internal data structures. If a higher recording is encountered at
        /// runtime, it is discarded and a warning is logged.
        /// </para>
        /// <para>
        /// This property defaults to 30000 milliseconds, i.e., 30 seconds.
        /// </para>
        /// </summary>
        /// <param name="highestLatencyMilliseconds"></param>
        /// <returns></returns>
        public DriverAppMetricsOptions SetHighestLatencyMilliseconds(int highestLatencyMilliseconds)
        {
            HighestLatencyMilliseconds = highestLatencyMilliseconds;
            return this;
        }
        
        /// <summary>
        /// The number of significant decimal digits to which internal structures will maintain
        /// value resolution and separation (for example, 3 means that recordings up to 1 second will be recorded with a
        /// resolution of 1 millisecond or better).
        /// This must be between 0 and 5. If the value is out of range, an exception is thrown.
        /// </summary>
        public DriverAppMetricsOptions SetSignificantDigits(int digits)
        {
            if (digits < 0 || digits > 5)
            {
                throw new ArgumentException("Significant digits must be an integer between 0 and 5.");
            }

            SignificantDigits = digits;
            return this;
        }

        /// <summary>
        /// Time unit to use for Timer metrics. This property defaults to <see cref="TimeUnit.Nanoseconds"/>.
        /// </summary>
        public DriverAppMetricsOptions SetTimersTimeUnit(TimeUnit timeUnit)
        {
            TimersTimeUnit = timeUnit;
            return this;
        }

        /// <summary>
        /// <para>
        /// The interval at which percentile data is refreshed.
        /// </para>
        /// <para>
        /// The driver records latency data in a "live" histogram, and serves results from a cached
        /// snapshot. Each time the snapshot gets older than the interval, the two are switched.
        /// Note that this switch happens upon fetching the metrics, so if you never fetch the
        /// recording interval might grow higher (that shouldn't be an issue in a production
        /// environment because you would typically have a metrics reporter that exports to a
        /// monitoring tool at a regular interval).
        /// </para> 
        /// <para>
        /// In practice, this means that if you set this to 10 seconds, you're looking at data from a 
        /// 10-second interval in the past, that is at most 10 seconds old. If you fetch the metrics 
        /// at a faster pace, you will observe the same data for 10 seconds until the interval expires.
        /// </para>
        /// <para>
        /// This property defaults to 10000 milliseconds, i.e., 10 seconds.
        /// </para>
        /// </summary>
        /// <param name="refreshIntervalMilliseconds"></param>
        /// <returns></returns>
        public DriverAppMetricsOptions SetRefreshIntervalMilliseconds(int refreshIntervalMilliseconds)
        {
            RefreshIntervalMilliseconds = refreshIntervalMilliseconds;
            return this;
        }
    }
}