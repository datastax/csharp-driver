namespace Cassandra
{
    internal class EventResponse : AbstractResponse
    {
        public const byte OpCode = 0x0C;

        public CassandraEventArgs CassandraEventArgs;

        internal EventResponse(ResponseFrame frame)
            : base(frame)
        {
            var eventTypeString = BEBinaryReader.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                var ce = new TopopogyChangeEventArgs();
                ce.What = BEBinaryReader.ReadString() == "NEW_NODE"
                              ? TopopogyChangeEventArgs.Reason.NewNode
                              : TopopogyChangeEventArgs.Reason.RemovedNode;
                ce.Address = BEBinaryReader.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            else if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = BEBinaryReader.ReadString() == "UP"
                              ? StatusChangeEventArgs.Reason.Up
                              : StatusChangeEventArgs.Reason.Down;
                ce.Address = BEBinaryReader.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            else if (eventTypeString == "SCHEMA_CHANGE")
            {
                var ce = new SchemaChangeEventArgs();
                var m = BEBinaryReader.ReadString();
                ce.What = m == "CREATED"
                              ? SchemaChangeEventArgs.Reason.Created
                              : (m == "UPDATED"
                                     ? SchemaChangeEventArgs.Reason.Updated
                                     : SchemaChangeEventArgs.Reason.Dropped);
                ce.Keyspace = BEBinaryReader.ReadString();
                ce.Table = BEBinaryReader.ReadString();
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
