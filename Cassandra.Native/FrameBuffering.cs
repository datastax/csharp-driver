using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class FrameBuffering : NoBuffering
    {
        int bodyLen = int.MaxValue;

        public override IEnumerable<ResponseFrame> Process(byte[] buffer, int size, Stream stream, IProtoBufComporessor compressor)
        {
            Init(buffer, size);

            while (AreMore())
            {

                byte b = GetByte();

                switch (byteIdx)
                {
                    case 0: tmpFrameHeader.version = b; break;
                    case 1: tmpFrameHeader.flags = b; break;
                    case 2: tmpFrameHeader.streamId = b; break;
                    case 3: tmpFrameHeader.opcode = b; break;
                    case 4:
                        {
                            tmpFrameHeader.len[0] = b;
                        } break;
                    case 5: tmpFrameHeader.len[1] = b; break;
                    case 6: tmpFrameHeader.len[2] = b; break;
                    case 7: tmpFrameHeader.len[3] = b;
                        bodyLen = ConversionHelper.FromBytesToInt32(tmpFrameHeader.len, 0);
                        tmpFrame = tmpFrameHeader.makeFrame(new BufferedProtoBuf(bodyLen, ((tmpFrameHeader.flags & 0x01) == 0x01) ? compressor : null));
                        yield return tmpFrame;
                        break;
                    default:
                        {
                            tmpFrame.RawStream.WriteByte(b);
                        }
                        break;
                }
                byteIdx++;
                if (byteIdx - 8 >= bodyLen)
                {
                    byteIdx = 0;
                    bodyLen = int.MaxValue;
                    tmpFrameHeader = new FrameHeader();
                }
            }
        }

        override public void Close()
        {
            if (tmpFrame != null)
                tmpFrame.RawStream.Write(null, 0, 0);
        }

        public override int PreferedBufferSize()
        {
            return 128 * 1024;
        }

        public override bool AllowSyncCompletion()
        {
            return true;
        }
    }
}
