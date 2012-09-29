using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class OptionsRequest : IRequest
    {
        public const byte OpCode = 0x05;
        
        int streamId;
        public OptionsRequest(int streamId)
        {
            this.streamId = streamId;
        }

        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            return wb.GetFrame();
        }
    }
}
