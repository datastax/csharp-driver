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
        public enum RetryDecisionType
        {
            Retry,
            Rethrow,
            Ignore
        };

        private readonly ConsistencyLevel? _retryCl;
        private readonly RetryDecisionType _type;

        /// <summary>
        ///  Gets the type of this retry decision.
        /// </summary>
        public RetryDecisionType DecisionType
        {
            get { return _type; }
        }

        /// <summary>
        ///  Gets the consistency level for a retry decision or <c>null</c> if
        ///  this retry decision is an <c>Ignore</c> or a
        ///  <c>Rethrow</c>.</summary>
        public ConsistencyLevel? RetryConsistencyLevel
        {
            get { return _retryCl; }
        }

        private RetryDecision(RetryDecisionType type, ConsistencyLevel? retryCL)
        {
            _type = type;
            _retryCl = retryCL;
        }

        /// <summary>
        ///  Creates a Rethrow retry decision.
        /// </summary>
        /// 
        /// <returns>a Rethrow retry decision.</returns>
        public static RetryDecision Rethrow()
        {
            return new RetryDecision(RetryDecisionType.Rethrow, QueryOptions.DefaultConsistencyLevel);
        }

        /// <summary>
        ///  Creates a Retry retry decision using the provided consistency level.
        /// </summary>
        /// <param name="consistency"> the consistency level to use for the retry.
        ///  </param>
        /// 
        /// <returns>a Retry with consistency level <c>consistency</c> retry
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
            return new RetryDecision(RetryDecisionType.Ignore, QueryOptions.DefaultConsistencyLevel);
        }
    }
}