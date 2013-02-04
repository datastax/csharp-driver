namespace Cassandra
{
    internal class ErrorResponse : AbstractResponse
    {
        public const byte OpCode = 0x00;
        public OutputError Output;

        internal ErrorResponse(ResponseFrame frame) : base(frame)
        {
            var ctype = (CassandraErrorType) BEBinaryReader.ReadInt32();
            var message = BEBinaryReader.ReadString();
            Output = OutputError.CreateOutputError(ctype, message, BEBinaryReader);
        }

        internal static ErrorResponse Create(ResponseFrame frame)
        {
            return new ErrorResponse(frame);
        }
    }
}
