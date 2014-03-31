namespace Cassandra
{
    internal class OutputTruncateError : OutputError
    {
        public override DriverException CreateException()
        {
            return new TruncateException(Message);
        }
    }
}