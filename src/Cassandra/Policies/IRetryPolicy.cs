namespace Cassandra
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
        ///  <p> Note that this method may be called even if <c>requiredResponses >=
        ///  receivedResponses</c> if <c>dataPresent</c> is <c>false</c>
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
        /// <returns>the retry decision. If <c>RetryDecision.Rethrow</c> is
        ///  returned, a
        ///  <link>com.datastax.driver.core.exceptions.ReadTimeoutException</link> will be
        ///  thrown for the operation.</returns>
        RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry);

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
        /// <returns>the retry decision. If <c>RetryDecision.Rethrow</c> is
        ///  returned, a
        ///  <link>com.datastax.driver.core.exceptions.WriteTimeoutException</link> will
        ///  be thrown for the operation.</returns>
        RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry);

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
        /// <returns>the retry decision. If <c>RetryDecision.Rethrow</c> is
        ///  returned, an
        ///  <link>com.datastax.driver.core.exceptions.UnavailableException</link> will be
        ///  thrown for the operation.</returns>
        RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry);
    }
}