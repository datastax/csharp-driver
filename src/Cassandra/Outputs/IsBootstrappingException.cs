namespace Cassandra
{
    public class IsBootstrappingException : QueryValidationException
    {
        public IsBootstrappingException(string message) : base(message)
        {
        }
    }
}