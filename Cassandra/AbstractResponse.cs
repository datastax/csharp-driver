using System;

namespace Cassandra
{
    internal class AbstractResponse
    {
        protected BEBinaryReader BEBinaryReader;
        public Guid? TraceID = null;

        internal AbstractResponse(ResponseFrame frame)
        {
            BEBinaryReader = new BEBinaryReader(frame);
            if ((frame.FrameHeader.Flags & 0x02) == 0x02)
            {
                var buffer = new byte[16];
                BEBinaryReader.Read(buffer,0,16);
                TraceID = ConversionHelper.ToGuidFromBigEndianBytes(buffer);
            }
        }
    }
}
