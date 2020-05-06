//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;

namespace Cassandra.Requests
{
    internal class StartupRequest : BaseRequest
    {
        public const byte StartupOpCode = 0x01;
        private readonly IReadOnlyDictionary<string, string> _options;

        public StartupRequest(IReadOnlyDictionary<string, string> startupOptions) : base(false, null)
        {
            _options = startupOptions ?? throw new ArgumentNullException(nameof(startupOptions));
        }

        protected override byte OpCode => StartupRequest.StartupOpCode;

        /// <inheritdoc />
        public override ResultMetadata ResultMetadata => null;

        protected override void WriteBody(FrameWriter wb)
        {
            wb.WriteUInt16((ushort)_options.Count);
            foreach (var kv in _options)
            {
                wb.WriteString(kv.Key);
                wb.WriteString(kv.Value);
            }
        }
    }
}