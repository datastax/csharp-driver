using System;

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