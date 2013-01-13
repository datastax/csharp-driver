using System.Collections.Generic;

namespace Cassandra
{
    internal class StartupRequest : IRequest
    {
        public const byte OpCode = 0x01;

        readonly int _streamId;
        readonly IDictionary<string, string> _options;
        public StartupRequest(int streamId, IDictionary<string,string> options)
        {
            this._streamId = streamId;
            this._options = options;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteUInt16((ushort)_options.Count);
            foreach(var kv in _options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}
