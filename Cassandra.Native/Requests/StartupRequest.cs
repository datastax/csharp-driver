using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class StartupRequest : IRequest
    {
        public const byte OpCode = 0x01;
        
        int streamId;
        IDictionary<string, string> options;
        public StartupRequest(int streamId, IDictionary<string,string> options)
        {
            this.streamId = streamId;
            this.options = options;
        }

        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteUInt16((ushort)options.Count);
            foreach(var kv in options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}
