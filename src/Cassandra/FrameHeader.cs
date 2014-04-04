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

namespace Cassandra
{
    internal class FrameHeader
    {
        public const int MaxFrameSize = 256*1024*1024;
        public const int Size = 8;
        public byte Flags;
        public byte[] Len = new byte[4];
        public byte Opcode;
        public byte StreamId;
        public byte Version;

        public ResponseFrame MakeFrame(IProtoBuf stream)
        {
            int bodyLen = TypeInterpreter.BytesToInt32(Len, 0);

            if (MaxFrameSize - 8 < bodyLen) throw new DriverInternalError("Frame length mismatch");

            var frame = new ResponseFrame {FrameHeader = this, RawStream = stream};
            return frame;
        }
    }
}