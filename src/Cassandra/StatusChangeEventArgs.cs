using System.Net;

namespace Cassandra
{
    public class StatusChangeEventArgs: CassandraEventArgs
    {
        public enum Reason
        {
            Up,
            Down
        };
        public Reason What;
        public IPAddress Address;
    }
}