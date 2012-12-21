using System;
using System.Collections.Generic;
using System.Text;
using System.Net;

namespace Cassandra.Native
{
    internal class EventResponse : IResponse
    {
        public const byte OpCode = 0x0C;

        public CassandraEventArgs CassandraEventArgs = new CassandraEventArgs();

        internal EventResponse(ResponseFrame frame)
        {
            var rd = new BEBinaryReader(frame);
            var eventTypeString = rd.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                CassandraEventArgs.CassandraEventType = CassandraEventType.TopologyChange;
                CassandraEventArgs.Message = rd.ReadString();
                CassandraEventArgs.IPAddress = rd.ReadInet().Address;
                return;
            }
            else if (eventTypeString == "STATUS_CHANGE")
            {
                CassandraEventArgs.CassandraEventType = CassandraEventType.StatusChange;
                CassandraEventArgs.Message = rd.ReadString();
                CassandraEventArgs.IPAddress = rd.ReadInet().Address;
                return;
            }
            else if (eventTypeString == "SCHEMA_CHANGE")
            {
                CassandraEventArgs.CassandraEventType = CassandraEventType.SchemaChange;
                CassandraEventArgs.Message = rd.ReadString();
                CassandraEventArgs.Message += " Affected keyspace:" + rd.ReadString();
                CassandraEventArgs.Message += " Affected table:" + rd.ReadString();
                return;
            }

            throw new CassandraClientProtocolViolationException("Unknown Event Type");
        }

        internal static EventResponse Create(ResponseFrame frame)
        {
            return new EventResponse(frame);
        }
    }
}
