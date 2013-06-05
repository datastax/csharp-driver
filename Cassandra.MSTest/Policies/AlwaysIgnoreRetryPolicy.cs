namespace Cassandra.MSTest
{
    public class AlwaysIgnoreRetryPolicy : IRetryPolicy
    {

        public static readonly AlwaysIgnoreRetryPolicy Instance = new AlwaysIgnoreRetryPolicy();

        private AlwaysIgnoreRetryPolicy() { }


        public RetryDecision OnReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            return RetryDecision.Ignore();
        }

        public RetryDecision OnWriteTimeout(Query query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Ignore();
        }

        public RetryDecision OnUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Ignore();
        }
    }
}