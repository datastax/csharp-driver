//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
﻿using System.Collections.Generic;
using System.IO;

namespace Cassandra
{
    internal class NoBuffering : IBuffering
    {
        byte[] _tempBuffer = null;
        int _curpos = 0;
        int _tempSize = 0;

        byte[] _newBuffer = null;
        int _newSize = 0;
        bool _bufferLoaded = false;

        protected void Init(byte[] buffer, int size)
        {
            if (_tempBuffer == null)
            {
                _bufferLoaded = true;
                _tempBuffer = buffer;
                _tempSize = size;
                _curpos = 0;
            }
            else
            {
                _bufferLoaded = false;
                _newBuffer = buffer;
                _newSize = size;
            }
        }

        protected byte GetByte()
        {
            if (_curpos >= _tempSize)
            {
                _bufferLoaded = true;
                _curpos = 0;
                _tempBuffer = _newBuffer;
                _tempSize = _newSize;
                _newBuffer = null;
            }
            var ret = _tempBuffer[_curpos];
            _curpos++;
            return ret;
        }

        protected bool AreMore()
        {
            return !_bufferLoaded || _curpos < _tempSize;
        }

        protected FrameHeader TmpFrameHeader = new FrameHeader();
        protected ResponseFrame TmpFrame;
        protected int ByteIdx = 0;

        virtual public IEnumerable<ResponseFrame> Process(byte[] buffer, int size, Stream stream, IProtoBufComporessor compressor)
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
                        TypeInterpreter.BytesToInt32(TmpFrameHeader.Len, 0);
                        TmpFrame = TmpFrameHeader.MakeFrame(new StreamProtoBuf(stream, ((TmpFrameHeader.Flags & 0x01) == 0x01) ? compressor : null));
                        yield return TmpFrame;
                        break;
                    default:
                        throw new DriverInternalError("Invalid state");
                }

                ByteIdx++;
                if (ByteIdx >= FrameHeader.Size)
                {
                    ByteIdx = 0;
                    TmpFrameHeader = new FrameHeader();
                }
            }
        }

        virtual public void Close()
        {
            if(TmpFrame!=null)
                TmpFrame.RawStream.Write(null, 0, 0);
        }

        public void Reset()
        {
            _tempBuffer = null;
            _curpos = 0;
            _tempSize = 0;
            _newBuffer = null;
            _newSize = 0;
            _bufferLoaded = false;
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
