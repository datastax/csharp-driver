namespace Cassandra
{
    internal class OutputSyntaxError : OutputError
    {
        public override DriverException CreateException()
        {
            return new SyntaxError(Message);
        }
    }
}