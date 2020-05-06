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
using Cassandra.Requests;
using Cassandra.Responses;

namespace Cassandra
{
    /// <summary>
    /// Parses the frame into a response
    /// </summary>
    internal class FrameParser
    {
        /// <summary>
        /// A factory to get the response handlers 
        /// </summary>
        private static readonly Dictionary<byte, Func<Frame, Response>> _responseHandlerFactory = 
            new Dictionary<byte, Func<Frame, Response>>
        {
            {AuthenticateResponse.OpCode, AuthenticateResponse.Create},
            {ErrorResponse.OpCode, ErrorResponse.Create},
            {EventResponse.OpCode, EventResponse.Create},
            {ReadyResponse.OpCode, ReadyResponse.Create},
            {ResultResponse.OpCode, ResultResponse.Create},
            {SupportedResponse.OpCode, SupportedResponse.Create},
            {AuthSuccessResponse.OpCode, AuthSuccessResponse.Create},
            {AuthChallengeResponse.OpCode, AuthChallengeResponse.Create}
        };

        /// <summary>
        /// Parses the response frame
        /// </summary>
        public static Response Parse(Frame frame)
        {
            byte opcode = frame.Header.Opcode;
            if (!_responseHandlerFactory.ContainsKey(opcode))
            {
                throw new DriverInternalError("Unknown Response Frame type " + opcode);
            }
            return _responseHandlerFactory[opcode](frame);
        }
    }
}
