//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse
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
