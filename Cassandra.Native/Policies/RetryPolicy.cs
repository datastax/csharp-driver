using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    /**
     * A policy that defines a default behavior to adopt when a request returns
     * a TimeoutException or an UnavailableException.
     *
     * Such policy allows to centralize the handling of query retries, allowing to
     * minimize the need for exception catching/handling in business code.
     */
    public interface RetryPolicy
    {
        /**
            * Defines whether to retry and at which consistency level on a read timeout.
            * <p>
            * Note that this method may be called even if
            * {@code requiredResponses >= receivedResponses} if {@code dataPresent} is
            * {@code false} (see
            * {@link com.datastax.driver.core.exceptions.ReadTimeoutException#wasDataRetrieved}).
            *
            * @param cl the original consistency level of the read that timeouted.
            * @param requiredResponses the number of responses that were required to
            * achieve the requested consistency level.
            * @param receivedResponses the number of responses that had been received
            * by the time the timeout exception was raised.
            * @param dataRetrieved whether actual data (by opposition to data checksum)
            * was present in the received responses.
            * @param nbRetry the number of retry already performed for this operation.
            * @return the retry decision. If {@code RetryDecision.Rethrow} is returned,
            * a {@link com.datastax.driver.core.exceptions.ReadTimeoutException} will
            * be thrown for the operation.
            */
        RetryDecision OnReadTimeout(ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry);

        /**
         * Defines whether to retry and at which consistency level on a write timeout.
         *
         * @param cl the original consistency level of the write that timeouted.
         * @param writeType the type of the write that timeouted.
         * @param requiredAcks the number of acknowledgments that were required to
         * achieve the requested consistency level.
         * @param receivedAcks the number of acknowledgments that had been received
         * by the time the timeout exception was raised.
         * @param nbRetry the number of retry already performed for this operation.
         * @return the retry decision. If {@code RetryDecision.Rethrow} is returned,
         * a {@link com.datastax.driver.core.exceptions.WriteTimeoutException} will
         * be thrown for the operation.
         */
        RetryDecision OnWriteTimeout(ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry);

        /**
         * Defines whether to retry and at which consistency level on an
         * unavailable exception.
         *
         * @param cl the original consistency level for the operation.
         * @param requiredReplica the number of replica that should have been
         * (known) alive for the operation to be attempted.
         * @param aliveReplica the number of replica that were know to be alive by
         * the coordinator of the operation.
         * @param nbRetry the number of retry already performed for this operation.
         * @return the retry decision. If {@code RetryDecision.Rethrow} is returned,
         * an {@link com.datastax.driver.core.exceptions.UnavailableException} will
         * be thrown for the operation.
         */
        RetryDecision OnUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry);
    }

    /**
             * A retry decision to adopt on a Cassandra exception (read/write timeout
             * or unavailable exception).
             * <p>
             * There is three possible decision:
             * <ul>
             *   <li>Rethrow: no retry should be attempted and an exception should be thrown</li>
             *   <li>Retry: the operation will be retried. The consistency level of the
             *   retry should be specified.</li>
             *   <li>Ignore: no retry should be attempted and the exception should be
             *   ignored. In that case, the operation that triggered the Cassandra
             *   exception will return an empty result set.</li>
             * </ul>
             */
    public class RetryDecision
    {
        /**
         * The type of retry decisions.
         */
        public enum RetryDecisionType { Retry, Rethrow, Ignore };

        private readonly RetryDecisionType _type;
        private readonly ConsistencyLevel? _retryCl;

        private RetryDecision(RetryDecisionType type, ConsistencyLevel? retryCL)
        {
            this._type = type;
            this._retryCl = retryCL;
        }

        /**
         * The type of this retry decision.
         *
         * @return the type of this retry decision.
         */
        public RetryDecisionType DecisionType { get { return _type; } }

        /**
         * The consistency level for a retry decision.
         *
         * @return the consistency level for a retry decision or {@code null}
         * if this retry decision is an {@code Ignore} or a {@code Rethrow}.
         */
        public ConsistencyLevel? RetryConsistencyLevel { get { return _retryCl; } }

        /**
         * Creates a Rethrow retry decision.
         *
         * @return a Rethrow retry decision.
         */
        public static RetryDecision Rethrow()
        {
            return new RetryDecision(RetryDecisionType.Rethrow, ConsistencyLevel.IGNORE);
        }

        /**
         * Creates a Retry retry decision using the provided consistency level.
         *
         * @param consistency the consistency level to use for the retry.
         * @return a Retry with consistency level {@code consistency} retry decision.
         */
        public static RetryDecision Retry(ConsistencyLevel? consistency)
        {
            return new RetryDecision(RetryDecisionType.Retry, consistency);
        }

        /**
         * Creates an Ignore retry decision.
         *
         * @return an Ignore retry decision.
         */
        public static RetryDecision Ignore()
        {
            return new RetryDecision(RetryDecisionType.Ignore, ConsistencyLevel.IGNORE);
        }
    }
}
