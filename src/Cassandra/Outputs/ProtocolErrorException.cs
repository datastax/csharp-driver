namespace Cassandra
{
    public class ProtocolErrorException : QueryValidationException
    {
        public ProtocolErrorException(string message) : base(message)
        {
        }
    }
}