namespace Cassandra
{
    public class OverloadedException : QueryValidationException
    {
        public OverloadedException(string Message) : base(Message)
        {
        }
    }
}