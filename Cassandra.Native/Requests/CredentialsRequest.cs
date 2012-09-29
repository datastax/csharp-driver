using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class CredentialsRequest : IRequest
    {
        public const byte OpCode = 0x04;
        int streamId;
        IDictionary<string, string> credentials;
        public CredentialsRequest(int streamId, IDictionary<string, string> credentials)
        {
            this.streamId = streamId;
            this.credentials = credentials;
        }
        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteUInt16((ushort)credentials.Count);
            foreach (var kv in credentials)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}
