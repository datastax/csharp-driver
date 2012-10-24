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
                CassandraEventArgs.IPEndPoint = rd.ReadInet();
                return;
            }
            else if (eventTypeString == "STATUS_CHANGE")
            {
                CassandraEventArgs.CassandraEventType = CassandraEventType.StatusChange;
                CassandraEventArgs.Message = rd.ReadString();
                CassandraEventArgs.IPEndPoint = rd.ReadInet();
                return;
            }

            throw new CassandraClientProtocolViolationException("Unknown Event Type");
        }
    }
}
