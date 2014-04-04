﻿//
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
    internal class ErrorResponse : AbstractResponse
    {
        public const byte OpCode = 0x00;
        public OutputError Output;

        internal ErrorResponse(ResponseFrame frame) : base(frame)
        {
            var ctype = (CassandraErrorType) BEBinaryReader.ReadInt32();
            string message = BEBinaryReader.ReadString();
            Output = OutputError.CreateOutputError(ctype, message, BEBinaryReader);
        }

        internal static ErrorResponse Create(ResponseFrame frame)
        {
            return new ErrorResponse(frame);
        }
    }
}