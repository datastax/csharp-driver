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

using System;

namespace Cassandra
{
    internal class FrameParser
    {
        private static readonly Func<ResponseFrame, AbstractResponse>[] ResponseFactoryMethods;

        static FrameParser()
        {
            ResponseFactoryMethods = new Func<ResponseFrame, AbstractResponse>[sbyte.MaxValue + 1];

            // TODO:  Replace with "enhanced enum" pattern?
            ResponseFactoryMethods[AuthenticateResponse.OpCode] = AuthenticateResponse.Create;
            ResponseFactoryMethods[ErrorResponse.OpCode] = ErrorResponse.Create;
            ResponseFactoryMethods[EventResponse.OpCode] = EventResponse.Create;
            ResponseFactoryMethods[ReadyResponse.OpCode] = ReadyResponse.Create;
            ResponseFactoryMethods[ResultResponse.OpCode] = ResultResponse.Create;
            ResponseFactoryMethods[SupportedResponse.OpCode] = SupportedResponse.Create;
            ResponseFactoryMethods[AuthSuccessResponse.OpCode] = AuthSuccessResponse.Create;
            ResponseFactoryMethods[AuthChallengeResponse.OpCode] = AuthChallengeResponse.Create;
        }

        public AbstractResponse Parse(ResponseFrame frame)
        {
            byte opcode = frame.FrameHeader.Opcode;
            if (ResponseFactoryMethods[opcode] != null)
                return ResponseFactoryMethods[opcode](frame);

            throw new DriverInternalError("Unknown Response Frame type");
        }
    }
}