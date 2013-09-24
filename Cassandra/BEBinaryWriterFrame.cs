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
﻿using System;
using System.IO;
using System.Diagnostics;

namespace Cassandra
{
    internal partial class BEBinaryWriter
    {
        private int _frameSizePos = -1;

        public void WriteFrameSize()
        {
            _frameSizePos = (int)_base.Seek(0, SeekOrigin.Current);            
            
            _base.BaseStream.Seek(4, SeekOrigin.Current); //Reserving space for "length of the frame body" value
            _base.BaseStream.SetLength(_base.BaseStream.Length + 4);
        }

        public void WriteFrameHeader(byte version, byte flags, byte streamId, byte opCode)
        {
            WriteByte(version);
            WriteByte(flags);
            WriteByte(streamId);
            WriteByte(opCode);
            WriteFrameSize();
        }


        public RequestFrame GetFrame()
        {
            var len = (int)_base.Seek(0, SeekOrigin.Current);
            Debug.Assert(_frameSizePos != -1);
            _base.Seek(_frameSizePos, SeekOrigin.Begin);
            WriteInt32(len - 8);
            return new RequestFrame() { Buffer = (MemoryTributary)_base.BaseStream };            
        }
    }
}
