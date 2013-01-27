namespace Cassandra
{
    internal class ReadyResponse : IResponse
    {
        public const byte OpCode = 0x02;
        internal ReadyResponse(ResponseFrame frame)
        {
        }
        internal static ReadyResponse Create(ResponseFrame frame)
        {
            return new ReadyResponse(frame);
        }
    }
}
