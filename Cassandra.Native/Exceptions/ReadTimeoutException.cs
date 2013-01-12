using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// A Cassandra timeout during a read query.
    /// </summary>
    public class ReadTimeoutException : QueryTimeoutException
    {
        public bool IsDataPresent { get; private set; }
        public ReadTimeoutException(string Message, ConsistencyLevel ConsistencyLevel, int Received, int BlockFor, bool IsDataPresent) :
            base(Message, ConsistencyLevel, Received, BlockFor) { this.IsDataPresent = IsDataPresent; }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return policy.OnReadTimeout(ConsistencyLevel, BlockFor, Received, IsDataPresent, queryRetries);
        }
    }
}