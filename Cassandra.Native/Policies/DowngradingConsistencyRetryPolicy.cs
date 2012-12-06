using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native.Policies
{

    /**
     * A retry policy that sometimes retry with a lower consistency level than
     * the one initially requested.
     * <p>
     * <b>BEWARE</b>: This policy may retry queries using a lower consistency
     * level than the one initially requested. By doing so, it may break
     * consistency guarantees. In other words, if you use this retry policy,
     * there is cases (documented below) where a read at {@code QUORUM}
     * <b>may not</b> see a preceding write at {@code QUORUM}. Do not use this
     * policy unless you have understood the cases where this can happen and
     * are ok with that. It is also highly recommended to always wrap this
     * policy into {@link LoggingRetryPolicy} to log the occurences of
     * such consistency break.
     * <p>
     * This policy implements the same retries than the {@link DefaultRetryPolicy}
     * policy. But on top of that, it also retries in the following cases:
     * <ul>
     *   <li>On a read timeout: if the number of replica that responded is
     *   greater than one but lower than is required by the requested
     *   consistency level, the operation is retried at a lower concistency
     *   level.</li>
     *   <li>On a write timeout: if the operation is an {@code
     *   WriteType.UNLOGGED_BATCH} and at least one replica acknowleged the
     *   write, the operation is retried at a lower consistency level.
     *   Furthermore, for other operation, if at least one replica acknowleged
     *   the write, the timeout is ignored.</li>
     *   <li>On an unavailable exception: if at least one replica is alive, the
     *   operation is retried at a lower consistency level.</li>
     * </ul>
     * <p>
     * The reasoning behing this retry policy is the following one. If, based
     * on the information the Cassandra coordinator node returns, retrying the
     * operation with the initally requested consistency has a change to
     * succeed, do it. Otherwise, if based on these informations we know <b>the
     * initially requested consistency level cannot be achieve currently</b>, then:
     * <ul>
     *   <li>For writes, ignore the exception (thus silently failing the
     *   consistency requirement) if we know the write has been persisted on at
     *   least one replica.</li>
     *   <li>For reads, try reading at a lower consistency level (thus silently
     *   failing the consistency requirement).</li>
     * </ul>
     * In other words, this policy implements the idea that if the requested
     * consistency level cannot be achieved, the next best thing for writes is
     * to make sure the data is persisted, and that reading something is better
     * than reading nothing, even if there is a risk of reading stale data.
     */
    public class DowngradingConsistencyRetryPolicy : RetryPolicy
    {

        public static DowngradingConsistencyRetryPolicy INSTANCE = new DowngradingConsistencyRetryPolicy();

        private DowngradingConsistencyRetryPolicy() { }



        public RetryDecision onReadTimeout(CqlConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            throw new NotImplementedException();
        }

        public RetryDecision onWriteTimeout(CqlConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            throw new NotImplementedException();
        }

        public RetryDecision onUnavailable(CqlConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            throw new NotImplementedException();
        }
    }

}