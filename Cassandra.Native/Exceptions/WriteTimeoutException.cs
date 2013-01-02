using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * A Cassandra timeout during a write query.
     */
    public class WriteTimeoutException : QueryTimeoutException
    {
        public string WriteType { get; private set; }
        public WriteTimeoutException(string Message, ConsistencyLevel ConsistencyLevel, int Received, int BlockFor, string WriteType) :
            base(Message, ConsistencyLevel, Received, BlockFor) { this.WriteType = WriteType; }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return policy.OnWriteTimeout(ConsistencyLevel, WriteType, BlockFor, Received, queryRetries);
        }
    }
}