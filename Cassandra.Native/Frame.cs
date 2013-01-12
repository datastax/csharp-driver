using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra
{

    internal class FrameHeader
    {
        public const int MaxFrameSize = 256 * 1024 * 1024;
        public const int Size = 8;
        public byte Version;
        public byte Flags;
        public byte StreamId;
        public byte Opcode;
        public byte[] Len = new byte[4];
        public ResponseFrame MakeFrame(IProtoBuf stream)
        {
            var bodyLen = ConversionHelper.FromBytesToInt32(Len, 0);

            if (MaxFrameSize - 8 < bodyLen) throw new DriverInternalError("Frame length mismatch");

            var frame = new ResponseFrame() { FrameHeader = this, RawStream = stream };
            return frame;
        }
    }
    
    internal class ResponseFrame
    {
        public FrameHeader FrameHeader;
        public IProtoBuf RawStream;
    }

    internal struct RequestFrame
    {
        public byte[] Buffer;

        public const int VersionIdx = 0;
        public const int FlagsIdx = 1;
        public const int StreamIdIdx = 2;
        public const int OpcodeIdIdx = 3;
        public const int LenIdx = 4;
        public const int BodyIdx = 8;

    }


}
