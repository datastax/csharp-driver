namespace Cassandra
{
    internal class OutputInvalid : OutputError
    {
        public override DriverException CreateException()
        {
            return new InvalidQueryException(Message);
        }
    }
}