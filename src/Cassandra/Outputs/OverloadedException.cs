namespace Cassandra
{
    public class OverloadedException : QueryValidationException
    {
        public OverloadedException(string message) : base(message)
        {
        }
    }
}