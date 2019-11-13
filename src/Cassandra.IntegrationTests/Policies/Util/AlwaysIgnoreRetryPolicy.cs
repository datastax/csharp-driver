//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
namespace Cassandra.IntegrationTests.Policies.Util
{
    public class AlwaysIgnoreRetryPolicy : IExtendedRetryPolicy
    {
        public static readonly AlwaysIgnoreRetryPolicy Instance = new AlwaysIgnoreRetryPolicy();

        private AlwaysIgnoreRetryPolicy()
        {
        }


        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                           int nbRetry)
        {
            return RetryDecision.Ignore();
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Ignore();
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Ignore();
        }

        public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
        {
            return RetryDecision.Ignore();
        }
    }
}
