//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Responses
{
    internal class AuthChallengeResponse : Response
    {
        public const byte OpCode = 0x0E;

        public byte[] Token;

        internal AuthChallengeResponse(Frame frame)
            : base(frame)
        {
            Token = Reader.ReadBytes();
        }

        internal static AuthChallengeResponse Create(Frame frame)
        {
            return new AuthChallengeResponse(frame);
        }
    }
}
