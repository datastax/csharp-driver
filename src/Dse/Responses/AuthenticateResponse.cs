//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Responses
{
    internal class AuthenticateResponse : Response
    {
        public const byte OpCode = 0x03;

        public string Authenticator;

        internal AuthenticateResponse(Frame frame) : base(frame)
        {
            Authenticator = Reader.ReadString();
        }

        internal static AuthenticateResponse Create(Frame frame)
        {
            return new AuthenticateResponse(frame);
        }
    }
}
