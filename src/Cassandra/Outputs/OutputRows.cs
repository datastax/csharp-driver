//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Threading;

namespace Cassandra
{
    internal class OutputRows : IOutput
    {
        public readonly int RowLength;
        private readonly Guid? _traceId;
        private RowSetMetadata _metadata;
        private byte _protocolVersion;

        /// <summary>
        /// Gets or sets the RowSet parsed from the response
        /// </summary>
        public RowSet RowSet { get; set; }

        public Guid? TraceId
        {
            get { return _traceId; }
        }

        internal OutputRows(byte protocolVersion, BEBinaryReader reader, bool buffered, Guid? traceId)
        {
            _protocolVersion = protocolVersion;
            _metadata = new RowSetMetadata(reader);
            RowLength = reader.ReadInt32();
            _traceId = traceId;
            RowSet = new RowSet();
            ProcessRows(RowSet, reader);
        }

        /// <summary>
        /// Process rows and sets the paging event handler
        /// </summary>
        internal virtual void ProcessRows(RowSet rs, BEBinaryReader reader)
        {
            if (this._metadata != null)
            {
                rs.Columns = _metadata.Columns;
                rs.PagingState = _metadata.PagingState;
            }
            for (var i = 0; i < this.RowLength; i++)
            {
                rs.AddRow(ProcessRowItem(reader));
            }
        }

        internal virtual Row ProcessRowItem(BEBinaryReader reader)
        {
            var valuesList = new List<byte[]>();
            for (var i = 0; i < _metadata.Columns.Length; i++ )
            {
                int length = reader.ReadInt32();
                if (length < 0)
                {
                    valuesList.Add(null);
                }
                else
                {
                    var buffer = new byte[length];
                    reader.Read(buffer, 0, length);
                    valuesList.Add(buffer);
                }
            }

            return new Row(_protocolVersion, valuesList.ToArray(), _metadata.Columns, _metadata.ColumnIndexes);
        }

        public void Dispose()
        {

        }
    }
}
