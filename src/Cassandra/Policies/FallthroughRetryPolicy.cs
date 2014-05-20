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
    ///  A retry policy that never retry (nor ignore). <p> All of the methods of this
    ///  retry policy unconditionally return
    ///  <link>RetryPolicy.RetryDecision#rethrow</link>. If this policy is used, retry
    ///  will have to be implemented in business code.</p>
    /// </summary>
    public class FallthroughRetryPolicy : IRetryPolicy
    {
        public static readonly FallthroughRetryPolicy Instance = new FallthroughRetryPolicy();

        private FallthroughRetryPolicy()
        {
        }


        /// <summary>
        ///  Defines whether to retry and at which consistency level on a read timeout.
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
        /// <returns><c>RetryDecision.rethrow()</c>.</returns>
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                           int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

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
        /// <returns><c>RetryDecision.rethrow()</c>.</returns>
        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

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
        /// <returns><c>RetryDecision.rethrow()</c>.</returns>
        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }
    }
}