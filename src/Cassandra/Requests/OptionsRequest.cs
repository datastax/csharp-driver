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

namespace Cassandra.Requests
{
    internal class OptionsRequest : BaseRequest
    {
        public const byte OptionsOpCode = 0x05;

        public OptionsRequest() : base(false, null)
        {
        }

        protected override byte OpCode => OptionsRequest.OptionsOpCode;

        /// <inheritdoc />
        public override ResultMetadata ResultMetadata => null;

        protected override void WriteBody(FrameWriter wb)
        {
            // OPTIONS requests have a header only
        }
    }
}