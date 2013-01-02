using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{

    internal class FrameHeader
    {
        public const int MaxFrameSize = 256 * 1024 * 1024;
        public const int Size = 8;
        public byte version;
        public byte flags;
        public byte streamId;
        public byte opcode;
        public byte[] len = new byte[4];
        public ResponseFrame makeFrame(IProtoBuf stream)
        {
            var bodyLen = ConversionHelper.FromBytesToInt32(len, 0);

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
        public byte[] buffer;

        public const int versionIdx = 0;
        public const int flagsIdx = 1;
        public const int streamIdIdx = 2;
        public const int opcodeIdIdx = 3;
        public const int lenIdx = 4;
        public const int bodyIdx = 8;

    }


}
