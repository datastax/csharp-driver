namespace Cassandra
{
    internal class EventResponse : IResponse
    {
        public const byte OpCode = 0x0C;

        public CassandraEventArgs CassandraEventArgs;

        internal EventResponse(ResponseFrame frame)
        {
            var rd = new BEBinaryReader(frame);
            var eventTypeString = rd.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                var ce = new TopopogyChangeEventArgs();
                ce.What = rd.ReadString() == "NEW_NODE"
                              ? TopopogyChangeEventArgs.Reason.NewNode
                              : TopopogyChangeEventArgs.Reason.RemovedNode;
                ce.Address = rd.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            else if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = rd.ReadString() == "UP"
                    ? StatusChangeEventArgs.Reason.Up
                    : StatusChangeEventArgs.Reason.Down;
                ce.Address = rd.ReadInet().Address;
                CassandraEventArgs = ce; 
                return;
            }
            else if (eventTypeString == "SCHEMA_CHANGE")
            {
                var ce = new SchemaChangeEventArgs();
                var m = rd.ReadString();
                ce.What = m == "CREATED"
                              ? SchemaChangeEventArgs.Reason.Created
                              : (m == "UPDATED"
                                     ? SchemaChangeEventArgs.Reason.Updated
                                     : SchemaChangeEventArgs.Reason.Dropped);
                ce.Keyspace = rd.ReadString();
                ce.Table = rd.ReadString();
                CassandraEventArgs = ce; 
                return;
            }

            throw new DriverInternalError("Unknown Event Type");
        }

        internal static EventResponse Create(ResponseFrame frame)
        {
            return new EventResponse(frame);
        }
    }
}
