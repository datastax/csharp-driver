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

namespace Cassandra
{
    /// <summary>
    /// Represents a reconnection policy that is possible to specify custom reconnection delays for each attempt.
    /// </summary>
    public class FixedReconnectionPolicy : IReconnectionPolicy
    {
        private readonly long[] _delays;

        /// <summary>
        /// Creates a new instance of a reconnection policy for which is possible to specify custom reconnection delays for each attempt.
        /// <para>The last delay provided will be used for the rest of the attempts.</para>
        /// </summary>
        public FixedReconnectionPolicy(params long[] delays)
        {
            if (delays == null)
            {
                throw new ArgumentNullException("delays");
            }
            if (delays.Length == 0)
            {
                throw new ArgumentException("You should provide at least one delay time in milliseconds");
            }
            _delays = delays;
        }

        /// <summary>
        /// Gets a copy of the provided <see cref="_delays"/> array.
        /// </summary>
        public long[] Delays => (long[])_delays.Clone();

        public IReconnectionSchedule NewSchedule()
        {
            return new FixedReconnectionSchedule(_delays);
        }

        private class FixedReconnectionSchedule : IReconnectionSchedule
        {
            private readonly long[] _delays;

            private int _index;

            public FixedReconnectionSchedule(params long[] delays)
            {
                _delays = delays;
            }

            public long NextDelayMs()
            {
                if (_index >= _delays.Length)
                {
                    return _delays[_delays.Length - 1];
                }
                return _delays[_index++];
            }
        }
    }
}
