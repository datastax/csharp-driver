using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra
{
    internal class QueryRequest : IRequest
    {
        public const byte OpCode = 0x07;

        readonly int _streamId;
        readonly string _cqlQuery;
        readonly ConsistencyLevel _consistency;

        public QueryRequest(int streamId, string cqlQuery, ConsistencyLevel consistency)
        {
            this._streamId = streamId;
            this._cqlQuery = cqlQuery;
            this._consistency = consistency;
            
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteLongString(_cqlQuery);
            wb.WriteInt16((short)_consistency);
            return wb.GetFrame();
        }
    }
}
