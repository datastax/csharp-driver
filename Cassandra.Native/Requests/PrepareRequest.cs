using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class PrepareRequest : IRequest
    {
        public const byte OpCode = 0x09;

        int streamId;
        string cqlQuery;
        public PrepareRequest(int streamId, string cqlQuery)
        {
            this.streamId = streamId;
            this.cqlQuery = cqlQuery;
        }
        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteLongString(cqlQuery);
            return wb.GetFrame();
        }
    }
}
