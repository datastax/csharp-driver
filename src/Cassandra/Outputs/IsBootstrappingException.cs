namespace Cassandra
{
    public class IsBootstrappingException : QueryValidationException
    {
        public IsBootstrappingException(string Message) : base(Message) { }
    }
}