namespace Cassandra
{
    internal class OutputConfigError : OutputError
    {
        public override DriverException CreateException()
        {
            return new InvalidConfigurationInQueryException(Message);
        }
    }
}