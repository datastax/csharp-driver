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
                    case 0: tmpFrameHeader.Version = b; break;
                    case 1: tmpFrameHeader.Flags = b; break;
                    case 2: tmpFrameHeader.StreamId = b; break;
                    case 3: tmpFrameHeader.Opcode = b; break;
                    case 4:
                        {
                            tmpFrameHeader.Len[0] = b;
                        } break;
                    case 5: tmpFrameHeader.Len[1] = b; break;
                    case 6: tmpFrameHeader.Len[2] = b; break;
                    case 7: tmpFrameHeader.Len[3] = b;
                        bodyLen = ConversionHelper.FromBytesToInt32(tmpFrameHeader.Len, 0);
                        tmpFrame = tmpFrameHeader.MakeFrame(new BufferedProtoBuf(bodyLen, ((tmpFrameHeader.Flags & 0x01) == 0x01) ? compressor : null));
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
