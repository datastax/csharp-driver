namespace Cassandra
{
    internal class SupportedResponse : AbstractResponse
    {
        public const byte OpCode = 0x06;
        public OutputOptions Output;

        internal SupportedResponse(ResponseFrame frame) : base(frame)
        {
            Output = new OutputOptions(BEBinaryReader);
        }

        internal static SupportedResponse Create(ResponseFrame frame)
        {
            return new SupportedResponse(frame);
        }
    }
}
