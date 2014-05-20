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
    ///  A reconnection policy that waits a constant time between each reconnection attempt.
    /// </summary>
    public class ConstantReconnectionPolicy : IReconnectionPolicy
    {
        private readonly long _delayMs;

        /// <summary>
        /// Gets the constant delay used by this reconnection policy. 
        /// </summary>
        public long ConstantDelayMs
        {
            get { return _delayMs; }
        }

        /// <summary>
        ///  Creates a reconnection policy that creates with the provided constant wait
        ///  time between reconnection attempts.
        /// </summary>
        /// <param name="constantDelayMs"> the constant delay in milliseconds to use.</param>
        public ConstantReconnectionPolicy(long constantDelayMs)
        {
            if (constantDelayMs > 0)
                _delayMs = constantDelayMs;
            else
                throw new ArgumentException("Constant delay time for reconnection policy have to be bigger than 0.");
        }

        /// <summary>
        ///  A new schedule that uses a constant <c>ConstantDelayMs</c> delay between reconnection attempt. 
        /// </summary>
        /// 
        /// <returns>the newly created schedule.</returns>
        public IReconnectionSchedule NewSchedule()
        {
            return new ConstantSchedule(this);
        }

        private class ConstantSchedule : IReconnectionSchedule
        {
            private readonly ConstantReconnectionPolicy _owner;

            internal ConstantSchedule(ConstantReconnectionPolicy owner)
            {
                _owner = owner;
            }

            public long NextDelayMs()
            {
                return _owner._delayMs;
            }
        }
    }
}