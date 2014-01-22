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

namespace Cassandra
{
    internal class StartupRequest : IRequest
    {
        public const byte OpCode = 0x01;

        readonly int _streamId;
        readonly IDictionary<string, string> _options;
        public StartupRequest(int streamId, IDictionary<string,string> options)
        {
            this._streamId = streamId;
            this._options = options;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(RequestFrame.ProtocolRequestVersionByte, 0x00, (byte)_streamId, OpCode);
            wb.WriteUInt16((ushort)_options.Count);
            foreach(var kv in _options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
            return wb.GetFrame();
        }
    }
}
