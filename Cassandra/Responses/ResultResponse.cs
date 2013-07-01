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
﻿namespace Cassandra
{
    internal class ResultResponse : AbstractResponse
    {
        public enum ResultResponseKind
        {
            Void = 1,
            Rows = 2,
            SetKeyspace = 3,
            Prepared = 4,
            SchemaChange = 5
        };

        public const byte OpCode = 0x08;
        public ResultResponseKind Kind;
        public IOutput Output;

        internal ResultResponse(ResponseFrame frame) : base(frame)
        {
            Kind = (ResultResponseKind) BEBinaryReader.ReadInt32();
            switch (Kind)
            {
                case ResultResponseKind.Void:
                    Output = new OutputVoid(TraceID);
                    break;
                case ResultResponseKind.Rows:
                    Output = new OutputRows(BEBinaryReader, frame.RawStream is BufferedProtoBuf, TraceID);
                    break;
                case ResultResponseKind.SetKeyspace:
                    Output = new OutputSetKeyspace(BEBinaryReader.ReadString());
                    break;
                case ResultResponseKind.Prepared:
                    Output = new OutputPrepared(BEBinaryReader);
                    break;
                case ResultResponseKind.SchemaChange:
                    Output = new OutputSchemaChange(BEBinaryReader, TraceID);
                    break;
                default:
                    throw new DriverInternalError("Unknown ResultResponseKind Type");
            }
        }

        internal static ResultResponse Create(ResponseFrame frame)
        {
            return new ResultResponse(frame);
        }
    }
}
