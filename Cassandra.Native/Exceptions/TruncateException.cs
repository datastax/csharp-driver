using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * Error during a truncation operation.
     */
    public class TruncateException : QueryExecutionException
    {
        public TruncateException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.Retry(null);
        }
    }
}