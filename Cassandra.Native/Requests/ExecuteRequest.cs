using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class ExecuteRequest : IRequest
    {
        public const byte OpCode = 0x0A;

        int streamId;
        object[] values;
        byte[] id;
        Metadata Metadata;

        public ExecuteRequest(int streamId, byte[] Id, Metadata Metadata, object[] values)
        {
            this.streamId = streamId;
            this.values = values;
            this.id = Id;
            this.Metadata = Metadata;
        }
        public RequestFrame GetFrame()
        {
            BEBinaryWriter wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)streamId, OpCode);
            wb.WriteInt16((short)id.Length);
            wb.WriteBytes(id);
            wb.WriteUInt16((ushort) values.Length);
            for(int i =0;i<Metadata.Columns.Length;i++)
            {
                var bytes = TypeInerpreter.InvCqlConvert(values[i], Metadata.Columns[i].type_code, Metadata.Columns[i].type_info);
                wb.WriteBytes(bytes);
            }
            return wb.GetFrame();
        }
    }
}
