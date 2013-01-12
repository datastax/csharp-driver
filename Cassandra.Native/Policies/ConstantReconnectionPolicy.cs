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

        private readonly long _delayMs;

        ///<summary>
        /// Creates a reconnection policy that creates with the provided constant wait
        /// time between reconnection attempts.
        ///
        /// <param name="constantDelayMs">the constant delay in milliseconds to use</param>
        ///</summary>
        public ConstantReconnectionPolicy(long constantDelayMs)
        {
            this._delayMs = constantDelayMs;
        }

        ///<summary>
        /// The constant delay used by this reconnection policy.
        ///</summary>
        public long ConstantDelayMs { get { return _delayMs; } }

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
            readonly ConstantReconnectionPolicy _owner;
            internal ConstantSchedule(ConstantReconnectionPolicy owner) { this._owner = owner; }

            public long NextDelayMs()
            {
                return _owner._delayMs;
            }
        }
    }

}