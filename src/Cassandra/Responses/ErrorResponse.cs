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

namespace Cassandra.Responses
{
    internal class ErrorResponse : Response
    {
        public const byte OpCode = 0x00;
        public OutputError Output;

        internal ErrorResponse(Frame frame)
            : base(frame)
        {
            int errorCode = Reader.ReadInt32();
            string message = Reader.ReadString();
            Output = OutputError.CreateOutputError(errorCode, message, Reader);
        }

        internal static ErrorResponse Create(Frame frame)
        {
            return new ErrorResponse(frame);
        }
    }
}