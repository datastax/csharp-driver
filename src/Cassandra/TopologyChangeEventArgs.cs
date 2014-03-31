using System.Net;

namespace Cassandra
{
    public class TopologyChangeEventArgs : CassandraEventArgs
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