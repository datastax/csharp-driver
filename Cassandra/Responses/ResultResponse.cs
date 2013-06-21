namespace Cassandra
{
    internal class ResultResponse : AbstractResponse
    {
        public enum ResultResponseKind
        {
            Void = 1,
            Rows = 2,
            SetKeyspace = 3,
            Prepared = 4,
            SchemaChange = 5
        };

        public const byte OpCode = 0x08;
        public ResultResponseKind Kind;
        public IOutput Output;

        internal ResultResponse(ResponseFrame frame) : base(frame)
        {
            Kind = (ResultResponseKind) BEBinaryReader.ReadInt32();
            switch (Kind)
            {
                case ResultResponseKind.Void:
                    Output = new OutputVoid(TraceID);
                    break;
                case ResultResponseKind.Rows:
                    Output = new OutputRows(BEBinaryReader, frame.RawStream is BufferedProtoBuf, TraceID);
                    break;
                case ResultResponseKind.SetKeyspace:
                    Output = new OutputSetKeyspace(BEBinaryReader.ReadString());
                    break;
                case ResultResponseKind.Prepared:
                    Output = new OutputPrepared(BEBinaryReader);
                    break;
                case ResultResponseKind.SchemaChange:
                    Output = new OutputSchemaChange(BEBinaryReader, TraceID);
                    break;
                default:
                    throw new DriverInternalError("Unknown ResultResponseKind Type");
            }
        }

        internal static ResultResponse Create(ResponseFrame frame)
        {
            return new ResultResponse(frame);
        }
    }
}
