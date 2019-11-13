//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.Responses
{
    internal class ErrorResponse : Response
    {
        public const byte OpCode = 0x00;
        public OutputError Output;

        internal ErrorResponse(Frame frame) 
            : base(frame)
        {
            int errorCode = Reader.ReadInt32();
            string message = Reader.ReadString();
            Output = OutputError.CreateOutputError(errorCode, message, Reader);
        }

        internal static ErrorResponse Create(Frame frame)
        {
            return new ErrorResponse(frame);
        }
    }
}
