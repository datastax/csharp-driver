//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.IntegrationTests.Policies.Util
{
    public class TryNextHostRetryPolicy : IRetryPolicy
    {
        public static readonly TryNextHostRetryPolicy Instance = new TryNextHostRetryPolicy();

        private TryNextHostRetryPolicy()
        {
        }

        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                           int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One, false);
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One, false);
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One, false);
        }
    }
}
