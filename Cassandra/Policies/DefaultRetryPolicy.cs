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
    ///  The default retry policy. <p> This policy retries queries in only two cases:
    ///  <ul> <li>On a read timeout, if enough replica replied but data was not
    ///  retrieved.</li> <li>On a write timeout, if we timeout while writting the
    ///  distributed log used by batch statements.</li> </ul> </p> <p> This retry policy is
    ///  conservative in that it will never retry with a different consistency level
    ///  than the one of the initial operation. </p><p> In some cases, it may be
    ///  convenient to use a more aggressive retry policy like
    ///  <link>DowngradingConsistencyRetryPolicy</link>.</p>
    /// </summary>
    public class DefaultRetryPolicy : IRetryPolicy
    {
        public DefaultRetryPolicy()
        {
        }

        /// <summary>
        ///  Defines whether to retry and at which consistency level on a read timeout.
        ///  <p> This method triggers a maximum of one retry, and only if enough replica
        ///  had responded to the read request but data was not retrieved amongst those.
        ///  Indeed, that case usually means that enough replica are alive to satisfy the
        ///  consistency but the coordinator picked a dead one for data retrieval, not
        ///  having detecte that replica as dead yet. The reasoning for retrying then is
        ///  that by the time we get the timeout the dead replica will likely have been
        ///  detected as dead and the retry has a high change of success.</p>
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
        /// <returns><code>RetryDecision.retry(cl)</code> if no retry attempt has yet
        ///  been tried and <code>receivedResponses >= requiredResponses &&
        ///  !dataRetrieved</code>, <code>RetryDecision.rethrow()</code>
        ///  otherwise.</returns>
        public RetryDecision OnReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses,
                                           bool dataRetrieved, int nbRetry)
        {
            if (nbRetry != 0)
                return RetryDecision.Rethrow();

            return receivedResponses >= requiredResponses && !dataRetrieved
                       ? RetryDecision.Retry(cl)
                       : RetryDecision.Rethrow();
        }

        /// <summary>
        ///  Defines whether to retry and at which consistency level on a write timeout.
        ///  <p> This method triggers a maximum of one retry, and only in the case of a
        ///  <code>WriteType.BATCH_LOG</code> write. The reasoning for the retry in that
        ///  case is that write to the distributed batch log is tried by the coordinator
        ///  of the write against a small subset of all the node alive in the local
        ///  datacenter. Hence, a timeout usually means that none of the nodes in that
        ///  subset were alive but the coordinator hasn't' detected them as dead. By the
        ///  time we get the timeout the dead nodes will likely have been detected as dead
        ///  and the retry has thus a high change of success.</p>
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
        /// <returns><code>RetryDecision.retry(cl)</code> if no retry attempt has yet
        ///  been tried and <code>writeType == WriteType.BATCH_LOG</code>,
        ///  <code>RetryDecision.rethrow()</code> otherwise.</returns>
        public RetryDecision OnWriteTimeout(Query query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks,
                                            int nbRetry)
        {
            if (nbRetry != 0)
                return RetryDecision.Rethrow();

            // If the batch log write failed, retry the operation as this might just be we were unlucky at picking candidtes
            return writeType == "BATCH_LOG" ? RetryDecision.Retry(cl) : RetryDecision.Rethrow();
        }

        /// <summary>
        ///  Defines whether to retry and at which consistency level on an unavailable
        ///  exception. <p> This method never retries as a retry on an unavailable
        ///  exception using the same consistency level has almost no change of success.</p>
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
        /// <returns><code>RetryDecision.rethrow()</code>.</returns>
        public RetryDecision OnUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }
    }
}
