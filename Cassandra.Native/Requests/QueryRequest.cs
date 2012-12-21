using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class QueryRequest : IRequest
    {
        public const byte OpCode = 0x07;

        int streamId;
        string cqlQuery;
        ConsistencyLevel consistency;

        public QueryRequest(int streamId, string cqlQuery, ConsistencyLevel consistency)
        {
            this.streamId = streamId;
            this.cqlQuery = cqlQuery;
            this.consistency = consistency;
            
        }

        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteLongString(cqlQuery);
            wb.WriteInt16((short)consistency);
            return wb.GetFrame();
        }
    }
}
