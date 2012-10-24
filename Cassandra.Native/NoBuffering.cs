using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    internal class NoBuffering : IBuffering
    {
        byte[] tempBuffer = null;
        int curpos = 0;
        int tempSize = 0;

        byte[] newBuffer = null;
        int newSize = 0;
        bool bufferLoaded = false;

        protected void Init(byte[] buffer, int size)
        {
            if (tempBuffer == null)
            {
                bufferLoaded = true;
                tempBuffer = buffer;
                tempSize = size;
                curpos = 0;
            }
            else
            {
                bufferLoaded = false;
                newBuffer = buffer;
                newSize = size;
            }
        }

        protected byte GetByte()
        {
            if (curpos >= tempSize)
            {
                bufferLoaded = true;
                curpos = 0;
                tempBuffer = newBuffer;
                tempSize = newSize;
                newBuffer = null;
            }
            var ret = tempBuffer[curpos];
            curpos++;
            return ret;
        }

        protected bool AreMore()
        {
            return !bufferLoaded || curpos < tempSize;
        }

        protected FrameHeader tmpFrameHeader = new FrameHeader();
        protected ResponseFrame tmpFrame;
        protected int byteIdx = 0;

        virtual public IEnumerable<ResponseFrame> Process(byte[] buffer, int size, Stream stream, IProtoBufComporessor compressor)
        {
            Init(buffer, size);

            int bodyLen = int.MaxValue;
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
                        tmpFrame = tmpFrameHeader.makeFrame(new StreamProtoBuf(stream, ((tmpFrameHeader.flags & 0x01) == 0x01) ? compressor : null));
                        yield return tmpFrame;
                        break;
                    default:
                        throw new CassandraClientProtocolViolationException("Invalid state");
                }

                byteIdx++;
                if (byteIdx >= FrameHeader.Size)
                {
                    byteIdx = 0;
                    tmpFrameHeader = new FrameHeader();
                }
            }
        }

        virtual public void Close()
        {
            if(tmpFrame!=null)
                tmpFrame.RawStream.Write(null, 0, 0);
        }

        public void Reset()
        {
            tempBuffer = null;
            curpos = 0;
            tempSize = 0;
            newBuffer = null;
            newSize = 0;
            bufferLoaded = false;
        }

        public virtual int PreferedBufferSize()
        {
            return FrameHeader.Size;
        }


        public virtual bool AllowSyncCompletion()
        {
            return false;
        }
    }
}
