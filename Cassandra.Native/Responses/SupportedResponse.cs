using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal class SupportedResponse : IResponse
    {
        public const byte OpCode = 0x06;
        public OutputOptions Output;

        internal SupportedResponse(ResponseFrame frame)
        {
            var rd = new BEBinaryReader(frame);
            Output = new OutputOptions(rd);
        }
        internal static SupportedResponse Create(ResponseFrame frame)
        {
            return new SupportedResponse(frame);
        }
    }
}
