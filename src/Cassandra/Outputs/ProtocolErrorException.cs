namespace Cassandra
{
    public class ProtocolErrorException : QueryValidationException
    {
        public ProtocolErrorException(string Message) : base(Message) { }
    }
}