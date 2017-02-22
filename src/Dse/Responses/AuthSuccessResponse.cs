//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Responses
{
    internal class AuthSuccessResponse : Response
    {
        public const byte OpCode = 0x10;

        public byte[] Token;

        internal AuthSuccessResponse(Frame frame)
            : base(frame)
        {
            Token = Reader.ReadBytes();
        }

        internal static AuthSuccessResponse Create(Frame frame)
        {
            return new AuthSuccessResponse(frame);
        }
    }
}
