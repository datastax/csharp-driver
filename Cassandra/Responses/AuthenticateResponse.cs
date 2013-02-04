namespace Cassandra
{
    internal class AuthenticateResponse : AbstractResponse
    {
        public const byte OpCode = 0x03;

        public string Authenticator;

        internal AuthenticateResponse(ResponseFrame frame) : base(frame)
        {
            Authenticator = BEBinaryReader.ReadString();
        }

        internal static AuthenticateResponse Create(ResponseFrame frame)
        {
            return new AuthenticateResponse(frame);
        }

    }
}
