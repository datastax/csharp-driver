using System;

namespace Cassandra
{
    /**
     * A reconnection policy that waits exponentially longer between each
     * reconnection attempt (but keeps a constant delay once a maximum delay is
     * reached).
     */
    public class ExponentialReconnectionPolicy : ReconnectionPolicy
    {
        private readonly long _baseDelayMs;
        private readonly long _maxDelayMs;

        /**
         * Creates a reconnection policy waiting exponentially longer for each new attempt.
         *
         * @param baseDelayMs the base delay in milliseconds to use for
         * the schedules created by this policy. 
         * @param maxDelayMs the maximum delay to wait between two attempts.
         */
        public ExponentialReconnectionPolicy(long baseDelayMs, long maxDelayMs)
        {
            this._baseDelayMs = baseDelayMs;
            this._maxDelayMs = maxDelayMs;
        }

        /**
         * The base delay in milliseconds for this policy (e.g. the delay before
         * the first reconnection attempt).
         *
         * @return the base delay in milliseconds for this policy.
         */
        public long BaseDelayMs
        {
            get
            {
                return _baseDelayMs;
            }
        }

        /**
         * The maximum delay in milliseconds between reconnection attempts for this policy.
         *
         * @return the maximum delay in milliseconds between reconnection attempts for this policy.
         */
        public long MaxDelayMs
        {
            get
            {
                return _maxDelayMs;
            }
        }

        public ReconnectionSchedule NewSchedule()
        {
            return new ExponentialSchedule(this);
        }
        private class ExponentialSchedule : ReconnectionSchedule
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