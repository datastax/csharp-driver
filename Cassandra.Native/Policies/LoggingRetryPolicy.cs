using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
{

    /**
     * A retry policy that wraps another policy, logging the decision made by its sub-policy.
     * <p>
     * Note that this policy only log the IGNORE and RETRY decisions (since
     * RETHROW decisions just amount to propate the cassandra exception). The
     * logging is done at the INFO level.
     */
    public class LoggingRetryPolicy : RetryPolicy
    {

        /**
         * Creates a new {@code RetryPolicy} that logs the decision of {@code policy}.
         *
         * @param policy the policy to wrap. The policy created by this constructor
         * will return the same decision than {@code policy} but will log them.
         */
        public LoggingRetryPolicy(RetryPolicy policy)
        {
        }

        public RetryDecision onReadTimeout(CqlConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            throw new NotImplementedException();
        }

        public RetryDecision onWriteTimeout(CqlConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            throw new NotImplementedException();
        }

        public RetryDecision onUnavailable(CqlConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            throw new NotImplementedException();
        }
    }

}