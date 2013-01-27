namespace Cassandra
{
    /// <summary>
    ///  Indicates a syntax error in a query.
    /// </summary>
    public class SyntaxError : QueryValidationException
    {
        public SyntaxError(string message) : base(message) { }
    }
}