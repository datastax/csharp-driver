namespace Cassandra
{
    public class ToManyConnectionsPerHost : DriverException
    {
        public ToManyConnectionsPerHost() : base("Maximum number of connections per host reached")
        {
        }
    }
}