namespace Cassandra
{
    internal class ExecuteRequest : IRequest
    {
        public const byte OpCode = 0x0A;

        readonly int _streamId;
        readonly object[] _values;
        readonly byte[] _id;
        readonly TableMetadata _metadata;
        readonly ConsistencyLevel _consistency;

        public ExecuteRequest(int streamId, byte[] id, TableMetadata metadata, object[] values, ConsistencyLevel consistency)
        {
            this._streamId = streamId;
            this._values = values;
            this._id = id;
            this._metadata = metadata;
            this._consistency = consistency;

        }
        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(0x01, 0x00, (byte)_streamId, OpCode);
            wb.WriteShortBytes(_id);
            wb.WriteUInt16((ushort) _values.Length);
            for(int i =0;i<_metadata.Columns.Length;i++)
            {
                var bytes = TypeInterpreter.InvCqlConvert(_values[i], _metadata.Columns[i].TypeCode, _metadata.Columns[i].TypeInfo);
                wb.WriteBytes(bytes);
            }
            wb.WriteInt16((short)_consistency);
            return wb.GetFrame();
        }
    }
}
