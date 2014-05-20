//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  A reconnection policy that waits exponentially longer between each
    ///  reconnection attempt (but keeps a constant delay once a maximum delay is
    ///  reached).
    /// </summary>
    public class ExponentialReconnectionPolicy : IReconnectionPolicy
    {
        private readonly long _baseDelayMs;
        private readonly long _maxAttempts;
        private readonly long _maxDelayMs;

        /// <summary>
        ///  Gets the base delay in milliseconds for this policy (e.g. the delay before the
        ///  first reconnection attempt).
        /// </summary>
        public long BaseDelayMs
        {
            get { return _baseDelayMs; }
        }

        /// <summary>
        ///  Gets the maximum delay in milliseconds between reconnection attempts for this
        ///  policy.
        /// </summary>
        public long MaxDelayMs
        {
            get { return _maxDelayMs; }
        }

        /// <summary>
        ///  Creates a reconnection policy waiting exponentially longer for each new
        ///  attempt.
        /// </summary>
        /// <param name="baseDelayMs"> the base delay in milliseconds to use for the
        ///  schedules created by this policy.  </param>
        /// <param name="maxDelayMs"> the maximum delay to wait between two
        ///  attempts.</param>
        public ExponentialReconnectionPolicy(long baseDelayMs, long maxDelayMs)
        {
            if (baseDelayMs < 0 || maxDelayMs < 0)
                throw new ArgumentOutOfRangeException("Invalid negative delay");
            if (baseDelayMs == 0)
                throw new ArgumentOutOfRangeException("baseDelayMs must be strictly positive");
            if (maxDelayMs < baseDelayMs)
                throw new ArgumentOutOfRangeException(string.Format("maxDelayMs (got {0}) cannot be smaller than baseDelayMs (got {1})", maxDelayMs,
                                                                    baseDelayMs));

            _baseDelayMs = baseDelayMs;
            _maxDelayMs = maxDelayMs;

            // Maximum number of attempts after which we overflow (which is kind of theoretical anyway, you'll'
            // die of old age before reaching that but hey ...)
            int ceil = (baseDelayMs & (baseDelayMs - 1)) == 0 ? 0 : 1;
            _maxAttempts = 64 - LeadingZeros(long.MaxValue/baseDelayMs) - ceil;
        }

        /// <summary>
        ///  A new schedule that used an exponentially growing delay between reconnection
        ///  attempts. <p> For this schedule, reconnection attempt <c>i</c> will be
        ///  tried <c>Math.min(2^(i-1) * BaseDelayMs, MaxDelayMs)</c>
        ///  milliseconds after the previous one.</p>
        /// </summary>
        /// 
        /// <returns>the newly created schedule.</returns>
        public IReconnectionSchedule NewSchedule()
        {
            return new ExponentialSchedule(this);
        }

        private static int LeadingZeros(long value)
        {
            int leadingZeros = 0;
            while (value != 0)
            {
                value = value >> 1;
                leadingZeros++;
            }
            return (64 - leadingZeros);
        }

        private class ExponentialSchedule : IReconnectionSchedule
        {
            private readonly ExponentialReconnectionPolicy _policy;

            private int _attempts;

            public ExponentialSchedule(ExponentialReconnectionPolicy policy)
            {
                _policy = policy;
            }

            public long NextDelayMs()
            {
                if (_attempts >= _policy._maxAttempts)
                    return _policy._maxDelayMs;

                return Math.Min(_policy._baseDelayMs*(1L << _attempts++), _policy._maxDelayMs);
            }
        }
    }
}