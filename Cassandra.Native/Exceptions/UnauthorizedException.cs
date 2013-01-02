using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * Indicates that a query cannot be performed due to the authorisation
     * restrictions of the logged user.
     */
    public class UnauthorizedException : QueryValidationException
    {
        public UnauthorizedException(string Message) : base(Message) { }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.rethrow();
        }
    }
}
