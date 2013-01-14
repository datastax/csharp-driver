namespace Cassandra
{
    /// <summary>
    ///  Error during a truncation operation.
    /// </summary>
    public class TruncateException : QueryExecutionException
    {
        public TruncateException(string message) : base(message) { }
    }
}