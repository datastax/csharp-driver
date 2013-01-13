namespace Cassandra
{
    internal class ErrorResponse : IResponse
    {
        public const byte OpCode = 0x00;
        public OutputError Output;
        internal ErrorResponse(ResponseFrame frame)
        {
            var cb = new BEBinaryReader(frame);
            var ctype = (CassandraErrorType)cb.ReadInt32();
            var message = cb.ReadString();
            Output = OutputError.CreateOutputError(ctype, message, cb);
        }

        internal static ErrorResponse Create(ResponseFrame frame)
        {
            return new ErrorResponse(frame);
        }
    }
}
