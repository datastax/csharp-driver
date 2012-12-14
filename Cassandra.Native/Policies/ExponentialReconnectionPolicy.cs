using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * A reconnection policy that waits exponentially longer between each
     * reconnection attempt (but keeps a constant delay once a maximum delay is
     * reached).
     */
    public class ExponentialReconnectionPolicy : ReconnectionPolicy
    {
        private readonly long baseDelayMs;
        private readonly long maxDelayMs;

        /**
         * Creates a reconnection policy waiting exponentially longer for each new attempt.
         *
         * @param baseDelayMs the base delay in milliseconds to use for
         * the schedules created by this policy. 
         * @param maxDelayMs the maximum delay to wait between two attempts.
         */
        public ExponentialReconnectionPolicy(long baseDelayMs, long maxDelayMs)
        {
            this.baseDelayMs = baseDelayMs;
            this.maxDelayMs = maxDelayMs;
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
                return baseDelayMs;
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
                return maxDelayMs;
            }
        }

        public ReconnectionSchedule NewSchedule()
        {
            return new ExponentialSchedule(this);
        }
        private class ExponentialSchedule : ReconnectionSchedule
        {
            ExponentialReconnectionPolicy policy;
            public ExponentialSchedule(ExponentialReconnectionPolicy policy)
            {
                this.policy = policy;
            }
            private int attempts;

            public long NextDelayMs()
            {
                // We "overflow" at 64 attempts but I doubt this matter
                if (attempts >= 64)
                    return policy.maxDelayMs;

                return Math.Min(policy.baseDelayMs * (1L << attempts++), policy.maxDelayMs);
            }
        }
    }
}