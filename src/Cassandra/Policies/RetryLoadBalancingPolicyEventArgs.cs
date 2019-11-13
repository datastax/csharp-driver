//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

﻿using System;

namespace Cassandra
{
    public class RetryLoadBalancingPolicyEventArgs : EventArgs
    {
        public bool Cancel = false;
        public long DelayMs { get; private set; }

        public RetryLoadBalancingPolicyEventArgs(long delayMs)
        {
            DelayMs = delayMs;
        }
    }
}