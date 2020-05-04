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

using Cassandra.Requests;

namespace Cassandra.Responses
{
    internal class ResultResponse : Response
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

        /// <summary>
        /// Cassandra result kind
        /// </summary>
        public ResultResponseKind Kind { get; private set; }

        /// <summary>
        /// Output of the result response based on the kind of result
        /// </summary>
        public IOutput Output { get; private set; }

        /// <summary>
        /// Is null if new_metadata_id is not set.
        /// </summary>
        public ResultMetadata NewResultMetadata { get; }

        internal ResultResponse(Frame frame) : base(frame)
        {
            Kind = (ResultResponseKind) Reader.ReadInt32();
            switch (Kind)
            {
                case ResultResponseKind.Void:
                    Output = new OutputVoid(TraceId);
                    break;
                case ResultResponseKind.Rows:
                    var outputRows = new OutputRows(Reader, frame.ResultMetadata, TraceId);
                    Output = outputRows;
                    if (outputRows.ResultRowsMetadata.HasNewResultMetadataId())
                    {
                        NewResultMetadata = new ResultMetadata(
                            outputRows.ResultRowsMetadata.NewResultMetadataId, outputRows.ResultRowsMetadata);
                    }
                    break;
                case ResultResponseKind.SetKeyspace:
                    Output = new OutputSetKeyspace(Reader.ReadString());
                    break;
                case ResultResponseKind.Prepared:
                    Output = new OutputPrepared(frame.Header.Version, Reader);
                    break;
                case ResultResponseKind.SchemaChange:
                    Output = new OutputSchemaChange(frame.Header.Version, Reader, TraceId);
                    break;
                default:
                    throw new DriverInternalError("Unknown ResultResponseKind Type");
            }
        }

        protected ResultResponse(ResultResponseKind kind, IOutput output)
        {
            Kind = kind;
            Output = output;
        }

        internal static ResultResponse Create(Frame frame)
        {
            return new ResultResponse(frame);
        }
    }
}
