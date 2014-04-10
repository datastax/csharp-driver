namespace Cassandra
{
    public class ServerErrorException : QueryValidationException
    {
        public ServerErrorException(string message) : base(message)
        {
        }
    }
}