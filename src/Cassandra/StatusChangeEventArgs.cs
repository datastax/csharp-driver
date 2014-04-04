using System.Net;

namespace Cassandra
{
    public class StatusChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            Up,
            Down
        };

        public IPAddress Address;
        public Reason What;
    }
}