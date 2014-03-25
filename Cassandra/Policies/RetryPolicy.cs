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
﻿namespace Cassandra
{
    /// <summary>
    ///  A policy that defines a default behavior to adopt when a request returns a
    ///  TimeoutException or an UnavailableException. Such policy allows to centralize
    ///  the handling of query retries, allowing to minimize the need for exception
    ///  catching/handling in business code.
    /// </summary>
    public interface IRetryPolicy
    {
        /// <summary>
        ///  Defines whether to retry and at which consistency level on a read timeout.
        ///  <p> Note that this method may be called even if <code>requiredResponses >=
        ///  receivedResponses</code> if <code>dataPresent</code> is <code>false</code>
        ///  (see <link>com.datastax.driver.core.exceptions.ReadTimeoutException#WasDataRetrieved</link>).</p>
        /// </summary>
        /// <param name="query"> the original query that timeouted. </param>
        /// <param name="cl"> the original consistency level of the read that timeouted.
        ///  </param>
        /// <param name="requiredResponses"> the number of responses that were required
        ///  to achieve the requested consistency level. </param>
        /// <param name="receivedResponses"> the number of responses that had been
        ///  received by the time the timeout exception was raised. </param>
        /// <param name="dataRetrieved"> whether actual data (by opposition to data
        ///  checksum) was present in the received responses. </param>
        /// <param name="nbRetry"> the number of retry already performed for this
        ///  operation. </param>
        /// 
        /// <returns>the retry decision. If <code>RetryDecision.Rethrow</code> is
        ///  returned, a
        ///  <link>com.datastax.driver.core.exceptions.ReadTimeoutException</link> will be
        ///  thrown for the operation.</returns>
        RetryDecision OnReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry);

        /// <summary>
        ///  Defines whether to retry and at which consistency level on a write timeout.
        /// </summary>
        /// <param name="query"> the original query that timeouted. </param>
        /// <param name="cl"> the original consistency level of the write that timeouted.
        ///  </param>
        /// <param name="writeType"> the type of the write that timeouted. </param>
        /// <param name="requiredAcks"> the number of acknowledgments that were required
        ///  to achieve the requested consistency level. </param>
        /// <param name="receivedAcks"> the number of acknowledgments that had been
        ///  received by the time the timeout exception was raised. </param>
        /// <param name="nbRetry"> the number of retry already performed for this
        ///  operation. </param>
        /// 
        /// <returns>the retry decision. If <code>RetryDecision.Rethrow</code> is
        ///  returned, a
        ///  <link>com.datastax.driver.core.exceptions.WriteTimeoutException</link> will
        ///  be thrown for the operation.</returns>
        RetryDecision OnWriteTimeout(Query query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry);

        /// <summary>
        ///  Defines whether to retry and at which consistency level on an unavailable
        ///  exception.
        /// </summary>
        /// <param name="query"> the original query for which the consistency level
        ///  cannot be achieved. </param>
        /// <param name="cl"> the original consistency level for the operation. </param>
        /// <param name="requiredReplica"> the number of replica that should have been
        ///  (known) alive for the operation to be attempted. </param>
        /// <param name="aliveReplica"> the number of replica that were know to be alive
        ///  by the coordinator of the operation. </param>
        /// <param name="nbRetry"> the number of retry already performed for this
        ///  operation. </param>
        /// 
        /// <returns>the retry decision. If <code>RetryDecision.Rethrow</code> is
        ///  returned, an
        ///  <link>com.datastax.driver.core.exceptions.UnavailableException</link> will be
        ///  thrown for the operation.</returns>
        RetryDecision OnUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry);
    }

    /// <summary>
    ///  A retry decision to adopt on a Cassandra exception (read/write timeout or
    ///  unavailable exception). <p> There is three possible decision: <ul>
    ///  <li>Rethrow: no retry should be attempted and an exception should be
    ///  thrown</li> <li>Retry: the operation will be retried. The consistency level
    ///  of the retry should be specified.</li> <li>Ignore: no retry should be
    ///  attempted and the exception should be ignored. In that case, the operation
    ///  that triggered the Cassandra exception will return an empty result set.</li></ul></p>
    /// </summary>
    public class RetryDecision
    {
        /// <summary>
        ///  The type of retry decisions.
        /// </summary>
        public enum RetryDecisionType { Retry, Rethrow, Ignore };

        private readonly RetryDecisionType _type;
        private readonly ConsistencyLevel? _retryCl;

        private RetryDecision(RetryDecisionType type, ConsistencyLevel? retryCL)
        {
            this._type = type;
            this._retryCl = retryCL;
        }

        /// <summary>
        ///  Gets the type of this retry decision.
        /// </summary>
        public RetryDecisionType DecisionType { get { return _type; } }

        /// <summary>
        ///  Gets the consistency level for a retry decision or <code>null</code> if
        ///  this retry decision is an <code>Ignore</code> or a
        ///  <code>Rethrow</code>.</summary>
        public ConsistencyLevel? RetryConsistencyLevel { get { return _retryCl; } }

        /// <summary>
        ///  Creates a Rethrow retry decision.
        /// </summary>
        /// 
        /// <returns>a Rethrow retry decision.</returns>
        public static RetryDecision Rethrow()
        {
            return new RetryDecision(RetryDecisionType.Rethrow, ConsistencyLevel.Default);
        }

        /// <summary>
        ///  Creates a Retry retry decision using the provided consistency level.
        /// </summary>
        /// <param name="consistency"> the consistency level to use for the retry.
        ///  </param>
        /// 
        /// <returns>a Retry with consistency level <code>consistency</code> retry
        ///  decision.</returns>
        public static RetryDecision Retry(ConsistencyLevel? consistency)
        {
            return new RetryDecision(RetryDecisionType.Retry, consistency);
        }

        /// <summary>
        ///  Creates an Ignore retry decision.
        /// </summary>
        /// 
        /// <returns>an Ignore retry decision.</returns>
        public static RetryDecision Ignore()
        {
            return new RetryDecision(RetryDecisionType.Ignore, ConsistencyLevel.Default);
        }
    }
}
