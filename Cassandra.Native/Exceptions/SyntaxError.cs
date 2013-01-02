using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * Indicates a syntax error in a query.
     */
    public class SyntaxError : QueryValidationException
    {
        public SyntaxError(string Message) : base(Message) { }

        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.rethrow();
        }
    }
}