using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra
{
    internal class OptionsRequest : IRequest
    {
        public const byte OpCode = 0x05;

        readonly int _streamId;
        public OptionsRequest(int streamId)
        {
            this._streamId = streamId;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            return wb.GetFrame();
        }
    }
}
