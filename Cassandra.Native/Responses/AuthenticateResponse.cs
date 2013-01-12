using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class AuthenticateResponse : IResponse
    {
        public const byte OpCode = 0x03;

        public string Authenticator;
        internal AuthenticateResponse(ResponseFrame frame)
        {
            var cb = new BEBinaryReader(frame);
            Authenticator = cb.ReadString();
        }

        internal static AuthenticateResponse Create(ResponseFrame frame)
        {
            return new AuthenticateResponse(frame);
        }

    }
}
