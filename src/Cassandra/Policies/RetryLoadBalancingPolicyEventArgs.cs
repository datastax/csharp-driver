using System;

namespace Cassandra
{
    public class RetryLoadBalancingPolicyEventArgs : EventArgs
    {
        public long DelayMs { get; private set; }
        public bool Cancel = false;
        public RetryLoadBalancingPolicyEventArgs(long delayMs)
        {
            this.DelayMs = delayMs;
        }
    }
}