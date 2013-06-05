namespace Cassandra.MSTest
{
    public class AlwaysRetryRetryPolicy : IRetryPolicy
    {
        public static readonly AlwaysRetryRetryPolicy Instance = new AlwaysRetryRetryPolicy();

        private AlwaysRetryRetryPolicy() { }

        public RetryDecision OnReadTimeout(Query query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnWriteTimeout(Query query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnUnavailable(Query query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }
    }
}