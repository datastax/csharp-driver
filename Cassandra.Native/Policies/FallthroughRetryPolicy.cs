using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
{
    /**
     * A retry policy that never retry (nor ignore).
     * <p>
     * All of the methods of this retry policy unconditionally return {@link RetryPolicy.RetryDecision#rethrow}.
     * If this policy is used, retry will have to be implemented in business code.
     */
    public class FallthroughRetryPolicy : RetryPolicy
    {

        public static readonly FallthroughRetryPolicy INSTANCE = new FallthroughRetryPolicy();

        private FallthroughRetryPolicy() { }


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