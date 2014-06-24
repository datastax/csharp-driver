using System.Net;

namespace Cassandra
{
    internal class TopologyChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            NewNode,
            RemovedNode
        };

        public IPAddress Address;
        public Reason What;
    }
}