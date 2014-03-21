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
using System.Reflection;

namespace Cassandra
{
    internal class FrameParser
    {
        delegate AbstractResponse MyDel(ResponseFrame frame);
        static readonly MyDel[] RegisteredResponses = new MyDel[sbyte.MaxValue + 1];

        static FrameParser()
        {
            Register(typeof(AuthenticateResponse));
            Register(typeof(ErrorResponse));
            Register(typeof(EventResponse));
            Register(typeof(ReadyResponse));
            Register(typeof(ResultResponse));
            Register(typeof(SupportedResponse));
            Register(typeof(AuthSuccessResponse));
            Register(typeof(AuthChallengeResponse));
        }

        static void Register(Type response)
        {
            var obj = response.GetField("OpCode").GetValue(response);
            var mth = response.GetMethod("Create", BindingFlags.NonPublic | BindingFlags.Static, null, new Type[] { typeof(ResponseFrame) }, null);
            RegisteredResponses[(byte)obj] = (MyDel)Delegate.CreateDelegate(typeof(MyDel), mth);
        }

        public AbstractResponse Parse(ResponseFrame frame)
        {
            var opcode = frame.FrameHeader.Opcode;
            if (RegisteredResponses[opcode] != null)
                return RegisteredResponses[opcode](frame);
            throw new DriverInternalError("Unknown Response Frame type");
        }
    }
}
