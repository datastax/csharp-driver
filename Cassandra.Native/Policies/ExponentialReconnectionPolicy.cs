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
        private readonly long _maxDelayMs;

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
            if (maxDelayMs < baseDelayMs)
                throw new ArgumentOutOfRangeException(string.Format("maxDelayMs (got {0}) cannot be smaller than baseDelayMs (got {1})", maxDelayMs, baseDelayMs)); 
            
            this._baseDelayMs = baseDelayMs;
            this._maxDelayMs = maxDelayMs;
        }

        /// <summary>
        ///  Gets the base delay in milliseconds for this policy (e.g. the delay before the
        ///  first reconnection attempt).
        /// </summary>
        public long BaseDelayMs
        {
            get
            {
                return _baseDelayMs;
            }
        }

        /// <summary>
        ///  Gets the maximum delay in milliseconds between reconnection attempts for this
        ///  policy.
        /// </summary>
        public long MaxDelayMs
        {
            get
            {
                return _maxDelayMs;
            }
        }

        /// <summary>
        ///  A new schedule that used an exponentially growing delay between reconnection
        ///  attempts. <p> For this schedule, reconnection attempt <code>i</code> will be
        ///  tried <code>Math.min(2^(i-1) * BaseDelayMs, MaxDelayMs)</code>
        ///  milliseconds after the previous one.</p>
        /// </summary>
        /// 
        /// <returns>the newly created schedule.</returns>
        public IReconnectionSchedule NewSchedule()
        {
            return new ExponentialSchedule(this);
        }
        private class ExponentialSchedule : IReconnectionSchedule
        {
            readonly ExponentialReconnectionPolicy _policy;
            public ExponentialSchedule(ExponentialReconnectionPolicy policy)
            {
                this._policy = policy;
            }

            private int _attempts;

            public long NextDelayMs()
            {
                // We "overflow" at 64 attempts but I doubt this matter
                if (_attempts >= 64)
                    return _policy._maxDelayMs;

                return Math.Min(_policy._baseDelayMs * (1L << _attempts++), _policy._maxDelayMs);
            }
        }
    }
}