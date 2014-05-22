//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

namespace Cassandra
{
    /// <summary>
    ///  A retry policy that wraps another policy, logging the decision made by its
    ///  sub-policy. <p> Note that this policy only log the Ignore and Retry decisions
    ///  (since Rethrow decisions just amount to propate the cassandra exception). The
    ///  logging is done at the Info level.</p>
    /// </summary>
    public class LoggingRetryPolicy : IRetryPolicy
    {
        private readonly Logger _logger = new Logger(typeof (LoggingRetryPolicy));
        private readonly IRetryPolicy _policy;

        /// <summary>
        ///  Creates a new <c>RetryPolicy</c> that logs the decision of
        ///  <c>policy</c>.
        /// </summary>
        /// <param name="policy"> the policy to wrap. The policy created by this
        ///  constructor will return the same decision than <c>policy</c> but will log them.</param>
        public LoggingRetryPolicy(IRetryPolicy policy)
        {
            _policy = policy;
        }

        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                           int nbRetry)
        {
            RetryDecision decision = _policy.OnReadTimeout(query, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Ignore:
                    _logger.Info(
                        string.Format(
                            "Ignoring read timeout (initial consistency: {0}, required responses: {1}, received responses: {2}, data retrieved: {3}, retries: {4})",
                            cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry));
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    _logger.Info(
                        string.Format(
                            "Retrying on read timeout at consistency {0} (initial consistency: {1}, required responses: {2}, received responses: {3}, data retrieved: {4}, retries: {5})",
                            CL(cl, decision), cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry));
                    break;
            }
            return decision;
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            RetryDecision decision = _policy.OnWriteTimeout(query, cl, writeType, requiredAcks, receivedAcks, nbRetry);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Ignore:
                    _logger.Info(
                        string.Format(
                            "Ignoring write timeout (initial consistency: {0}, write type: {1} required acknowledgments: {2}, received acknowledgments: {3}, retries: {4})",
                            cl, writeType, requiredAcks, receivedAcks, nbRetry));
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    _logger.Info(
                        string.Format(
                            "Retrying on write timeout at consistency {0}(initial consistency: {1}, write type: {2}, required acknowledgments: {3}, received acknowledgments: {4}, retries: {5})",
                            CL(cl, decision), cl, writeType, requiredAcks, receivedAcks, nbRetry));
                    break;
            }
            return decision;
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            RetryDecision decision = _policy.OnUnavailable(query, cl, requiredReplica, aliveReplica, nbRetry);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Ignore:
                    _logger.Info(
                        string.Format(
                            "Ignoring unavailable exception (initial consistency: {0}, required replica: {1}, alive replica: {2}, retries: {3})", cl,
                            requiredReplica, aliveReplica, nbRetry));
                    break;
                case RetryDecision.RetryDecisionType.Retry:
                    _logger.Info(
                        string.Format(
                            "Retrying on unavailable exception at consistency {0} (initial consistency: {1}, required replica: {2}, alive replica: {3}, retries: {4})",
                            CL(cl, decision), cl, requiredReplica, aliveReplica, nbRetry));
                    break;
            }
            return decision;
        }

        private static ConsistencyLevel CL(ConsistencyLevel cl, RetryDecision decision)
        {
            return decision.RetryConsistencyLevel ?? cl;
        }
    }
}