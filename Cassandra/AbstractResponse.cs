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

namespace Cassandra
{
    internal class AbstractResponse
    {
        protected BEBinaryReader BEBinaryReader;
        public Guid? TraceID = null;

        internal AbstractResponse(ResponseFrame frame)
        {
            BEBinaryReader = new BEBinaryReader(frame);
            if (frame.FrameHeader.Version != ResponseFrame.ProtocolResponseVersionByte)
                throw new ProtocolErrorException("Invalid protocol version");

            if ((frame.FrameHeader.Flags & 0x02) == 0x02)
            {
                var buffer = new byte[16];
                BEBinaryReader.Read(buffer, 0, 16);
                TraceID = new Guid(TypeInterpreter.GuidShuffle(buffer));
            }
        }
    }
}
