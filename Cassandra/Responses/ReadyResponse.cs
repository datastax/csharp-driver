namespace Cassandra
{
    internal class ReadyResponse : AbstractResponse
    {
        public const byte OpCode = 0x02;

        internal ReadyResponse(ResponseFrame frame)
            : base(frame)
        {
        }

        internal static ReadyResponse Create(ResponseFrame frame)
        {
            return new ReadyResponse(frame);
        }
    }
}
