namespace Cassandra
{
    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string Message) : base(Message)
        {
        }
    }
}