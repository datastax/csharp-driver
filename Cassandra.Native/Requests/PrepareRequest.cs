using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class PrepareRequest : IRequest
    {
        public const byte OpCode = 0x09;

        readonly int _streamId;
        readonly string _cqlQuery;
        public PrepareRequest(int streamId, string cqlQuery)
        {
            this._streamId = streamId;
            this._cqlQuery = cqlQuery;
        }
        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteLongString(_cqlQuery);
            return wb.GetFrame();
        }
    }
}
