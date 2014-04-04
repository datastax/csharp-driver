namespace Cassandra
{
    internal class OutputServerError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ServerErrorException(Message);
        }
    }
}