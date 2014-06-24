using System.Net;

namespace Cassandra
{
    internal class StatusChangeEventArgs : CassandraEventArgs
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