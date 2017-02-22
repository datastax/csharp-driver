//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Responses
{
    internal class SupportedResponse : Response
    {
        public const byte OpCode = 0x06;
        public OutputOptions Output;

        internal SupportedResponse(Frame frame) : base(frame)
        {
            Output = new OutputOptions(Reader);
        }

        internal static SupportedResponse Create(Frame frame)
        {
            return new SupportedResponse(frame);
        }
    }
}
