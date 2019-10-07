//
//      Copyright (C) DataStax Inc.
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
            /// <summary>
            /// the operation will be retried. The consistency level of the retry should be specified.
            /// </summary>
            Retry,
            /// <summary>
            /// no retry should be attempted and an exception should be thrown
            /// </summary>
            Rethrow,
            /// <summary>
            /// no retry should be attempted and the exception should be ignored. In that case, the operation that triggered the Cassandra exception will return an empty result set.
            /// </summary>
            Ignore
        };

        /// <summary>
        ///  Gets the type of this retry decision.
        /// </summary>
        public RetryDecisionType DecisionType { get; private set; }

        /// <summary>
        ///  Gets the consistency level for a retry decision or <c>null</c> if
        ///  this retry decision is an <c>Ignore</c> or a
        ///  <c>Rethrow</c>.</summary>
        public ConsistencyLevel? RetryConsistencyLevel { get; private set; }

        /// <summary>
        /// Determines whether the retry policy uses the same host for retry decision. Default: true.
        /// </summary>
        public bool UseCurrentHost { get; private set; }

        private RetryDecision(RetryDecisionType type, ConsistencyLevel? retryConsistencyLevel, bool useCurrentHost)
        {
            DecisionType = type;
            RetryConsistencyLevel = retryConsistencyLevel;
            UseCurrentHost = useCurrentHost;
        }

        /// <summary>
        ///  Creates a Rethrow retry decision.
        /// </summary>
        /// <returns>a Rethrow retry decision.</returns>
        public static RetryDecision Rethrow()
        {
            return new RetryDecision(RetryDecisionType.Rethrow, QueryOptions.DefaultConsistencyLevel, true);
        }

        /// <summary>
        ///  Creates a decision to retry using the provided consistency level.
        /// </summary>
        /// <param name="consistency"> the consistency level to use for the retry.</param>
        /// <param name="useCurrentHost">Determines if the retry is made using the current host.</param>
        /// <returns>a Retry with consistency level <c>consistency</c> retry
        ///  decision.</returns>
        public static RetryDecision Retry(ConsistencyLevel? consistency, bool useCurrentHost)
        {
            return new RetryDecision(RetryDecisionType.Retry, consistency, useCurrentHost);
        }

        /// <summary>
        ///  Creates a decision to retry using the provided consistency level on the same host.
        /// </summary>
        /// <param name="consistency"> the consistency level to use for the retry.</param>
        /// <returns>a Retry with consistency level <c>consistency</c> retry decision.</returns>
        public static RetryDecision Retry(ConsistencyLevel? consistency)
        {
            return Retry(consistency, true);
        }

        /// <summary>
        ///  Creates an Ignore retry decision.
        /// </summary>
        /// <returns>an Ignore retry decision.</returns>
        public static RetryDecision Ignore()
        {
            return new RetryDecision(RetryDecisionType.Ignore, QueryOptions.DefaultConsistencyLevel, true);
        }
    }
}
