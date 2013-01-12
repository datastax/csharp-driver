using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra
{
    internal class FrameBuffering : NoBuffering
    {
        int _bodyLen = int.MaxValue;

        public override IEnumerable<ResponseFrame> Process(byte[] buffer, int size, Stream stream, IProtoBufComporessor compressor)
        {
            Init(buffer, size);

            while (AreMore())
            {

                byte b = GetByte();

                switch (ByteIdx)
                {
                    case 0: TmpFrameHeader.Version = b; break;
                    case 1: TmpFrameHeader.Flags = b; break;
                    case 2: TmpFrameHeader.StreamId = b; break;
                    case 3: TmpFrameHeader.Opcode = b; break;
                    case 4:
                        {
                            TmpFrameHeader.Len[0] = b;
                        } break;
                    case 5: TmpFrameHeader.Len[1] = b; break;
                    case 6: TmpFrameHeader.Len[2] = b; break;
                    case 7: TmpFrameHeader.Len[3] = b;
                        _bodyLen = ConversionHelper.FromBytesToInt32(TmpFrameHeader.Len, 0);
                        TmpFrame = TmpFrameHeader.MakeFrame(new BufferedProtoBuf(_bodyLen, ((TmpFrameHeader.Flags & 0x01) == 0x01) ? compressor : null));
                        yield return TmpFrame;
                        break;
                    default:
                        {
                            TmpFrame.RawStream.WriteByte(b);
                        }
                        break;
                }
                ByteIdx++;
                if (ByteIdx - 8 >= _bodyLen)
                {
                    ByteIdx = 0;
                    _bodyLen = int.MaxValue;
                    TmpFrameHeader = new FrameHeader();
                }
            }
        }

        override public void Close()
        {
            if (TmpFrame != null)
                TmpFrame.RawStream.Write(null, 0, 0);
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
