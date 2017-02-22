//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
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
        private static readonly Dictionary<byte, Func<Frame, Response>> _responseHandlerFactory = new Dictionary<byte, Func<Frame, Response>>
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
