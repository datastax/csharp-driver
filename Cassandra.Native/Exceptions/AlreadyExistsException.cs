namespace Cassandra
{
    /**
     * Exception thrown when a query attemps to create a keyspace or table that already exists.
     */
    public class AlreadyExistsException : QueryValidationException
    {
        public string Ks { get; private set; }
        public string Table { get; private set; }

        public AlreadyExistsException(string Message, string Ks, string Table) :
            base(Message) { this.Ks = Ks; this.Table = Table; }
        public override RetryDecision GetRetryDecition(RetryPolicy policy, int queryRetries)
        {
            return RetryDecision.rethrow();
        }
    }
}