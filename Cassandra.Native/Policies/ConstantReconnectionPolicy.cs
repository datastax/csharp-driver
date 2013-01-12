using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    ///<summary>
    /// A reconnection policy that waits a constant time between each reconnection attempt.
    ///</summary>
    public class ConstantReconnectionPolicy : ReconnectionPolicy
    {

        private readonly long delayMs;

        ///<summary>
        /// Creates a reconnection policy that creates with the provided constant wait
        /// time between reconnection attempts.
        ///
        /// <param name="constantDelayMs">the constant delay in milliseconds to use</param>
        ///</summary>
        public ConstantReconnectionPolicy(long constantDelayMs)
        {
            this.delayMs = constantDelayMs;
        }

        ///<summary>
        /// The constant delay used by this reconnection policy.
        ///</summary>
        public long ConstantDelayMs { get { return delayMs; } }

        ///<summary>
        /// A new schedule that uses a constant {@code getConstantDelayMs()} delay
        /// between reconnection attempt.
        ///
        /// <returns>the newly created schedule</returns>
        ///</summary>
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