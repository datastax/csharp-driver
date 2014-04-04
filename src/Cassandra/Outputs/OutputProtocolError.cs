namespace Cassandra
{
    internal class OutputProtocolError : OutputError
    {
        public override DriverException CreateException()
        {
            return new ProtocolErrorException(Message);
        }
    }
}