using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * A reconnection policy that waits a constant time between each reconnection attempt.
     */
    public class ConstantReconnectionPolicy : ReconnectionPolicy
    {

        private readonly long delayMs;

        /**
         * Creates a reconnection policy that creates with the provided constant wait
         * time between reconnection attempts.
         *
         * @param constantDelayMs the constant delay in milliseconds to use.
         */
        public ConstantReconnectionPolicy(long constantDelayMs)
        {
            this.delayMs = constantDelayMs;
        }

        /**
         * The constant delay used by this reconnection policy.
         *
         * @return the constant delay used by this reconnection policy.
         */
        public long ConstantDelayMs { get { return delayMs; } }

        /**
         * A new schedule that uses a constant {@code getConstantDelayMs()} delay
         * between reconnection attempt.
         *
         * @return the newly created schedule.
         */
        public ReconnectionSchedule NewSchedule()
        {
            return new ConstantSchedule(this);
        }

        private class ConstantSchedule : ReconnectionSchedule
        {
            ConstantReconnectionPolicy owner;
            internal ConstantSchedule(ConstantReconnectionPolicy owner) { this.owner = owner; }

            public long NextDelayMs()
            {
                return owner.delayMs;
            }
        }
    }

}