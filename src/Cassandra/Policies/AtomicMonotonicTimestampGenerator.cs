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
using System.Threading;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// A timestamp generator that guarantees monotonically increasing timestamps among all client threads
    /// and logs warnings when timestamps drift in the future.
    /// </summary>
    public class AtomicMonotonicTimestampGenerator : ITimestampGenerator
    {
        /// <summary>
        /// Default warning threshold in milliseconds.
        /// </summary>
        public const int DefaultWarningThreshold = 1000;

        /// <summary>
        /// Default time separation between log events (expressed in milliseconds) in case of multiple log events.
        /// </summary>
        public const int DefaultMinLogInterval = 1000;

        /// <summary>
        /// The amount of ticks per microsecond.
        /// </summary>
        protected const long TicksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;

        /// <summary>
        /// The amount of ticks since 1/1/1 to unix epoch.
        /// </summary>
        protected static readonly long UnixEpochTicks = TypeSerializer.UnixStart.UtcTicks;

        internal static readonly Logger Logger = new Logger(typeof(AtomicMonotonicTimestampGenerator));
        private readonly int _warningThresholdMicros;
        private readonly long _minLogInterval;
        private readonly Logger _logger;
        private long _lastWarning;
        private long _lastValue;

        /// <summary>
        /// Creates a new instance of <see cref="AtomicMonotonicTimestampGenerator"/>.
        /// </summary>
        /// <param name="warningThreshold">
        /// Determines how far in the future timestamps are allowed to drift before a warning is logged, expressed
        /// in milliseconds. Default: <c>1000</c>
        /// </param>
        /// <param name="minLogInterval">
        /// In case of multiple log events, it determines the time separation between log events, expressed in 
        /// milliseconds. Use 0 to disable. Default: <c>1000</c>.
        /// </param>
        public AtomicMonotonicTimestampGenerator(
            int warningThreshold = DefaultWarningThreshold, 
            int minLogInterval = DefaultMinLogInterval) : this(warningThreshold, minLogInterval, Logger)
        {
            
        }

        /// <summary>
        /// Internal constructor to allow injection of the logger
        /// </summary>
        internal AtomicMonotonicTimestampGenerator(int warningThreshold, int minLogInterval, Logger logger)
        {
            if (warningThreshold <= 0)
            {
                throw new ArgumentException("Warning threshold should be greater than 0");
            }
            if (minLogInterval < 0)
            {
                throw new ArgumentException("Minimum log interval should a positive number or zero to disable logging");
            }
            // The parameter is expressed in millis and the field in micros
            _warningThresholdMicros = warningThreshold * 1000;
            _minLogInterval = minLogInterval;
            _logger = logger;
        }

        /// <summary>
        /// Retrieves the current system-clock time in expressed microseconds since UNIX epoch.
        /// </summary>
        protected virtual long GetTimestamp()
        {
            return (DateTime.UtcNow.Ticks - UnixEpochTicks) / TicksPerMicrosecond;
        }
        
        /// <inheritdoc />
        public long Next()
        {
            var lastValue = Volatile.Read(ref _lastValue);
            while (true)
            {
                var nextValue = GenerateNext(lastValue);
                var originalValue = Interlocked.CompareExchange(ref _lastValue, nextValue, lastValue);
                if (originalValue == lastValue)
                {
                    return nextValue;
                }
                lastValue = originalValue;
            }
        }

        private long GenerateNext(long lastValue)
        {
            var timestamp = GetTimestamp();
            if (timestamp > lastValue)
            {
                return timestamp;
            }
            OnDrift(timestamp, lastValue);
            return lastValue + 1;
        }

        private void OnDrift(long timestamp, long lastTimestamp)
        {
            if (_minLogInterval == 0)
            {
                // Logging disabled
                return;
            }
            if (lastTimestamp - timestamp < _warningThresholdMicros)
            {
                // It drifted but not enough to log a warning
                return;
            }
            var warningTimestamp = DateTime.UtcNow.Ticks/TimeSpan.TicksPerMillisecond;
            var lastWarning = Volatile.Read(ref _lastWarning);
            if (warningTimestamp - lastWarning <= _minLogInterval)
            {
                return;
            }
            if (Interlocked.CompareExchange(ref _lastWarning, warningTimestamp, lastWarning) == lastWarning)
            {
                _logger.Warning(
                    "Timestamp generated using current date was {0} milliseconds behind the last generated " +
                    "timestamp (which was {1}), the returned value ({2}) is being artificially incremented to " +
                    "guarantee monotonicity.", lastTimestamp - timestamp, lastTimestamp, lastTimestamp + 1);
            }
        }
    }
}
