using System.Collections.Generic;

namespace Cassandra
{
    internal class CredentialsRequest : IRequest
    {
        public const byte OpCode = 0x04;
        readonly int _streamId;
        readonly IDictionary<string, string> _credentials;
        public CredentialsRequest(int streamId, IDictionary<string, string> credentials)
        {
            this._streamId = streamId;
            this._credentials = credentials;
        }
        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteUInt16((ushort)_credentials.Count);
            foreach (var kv in _credentials)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}
