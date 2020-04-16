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
using System.Threading;
using Cassandra.Requests;

// ReSharper disable CheckNamespace
namespace Cassandra
{
    internal class OutputRows : IOutput
    {
        private readonly int _rowLength;
        private const int ReusableBufferLength = 1024;
        private static readonly ThreadLocal<byte[]> ReusableBuffer = new ThreadLocal<byte[]>(() => new byte[ReusableBufferLength]);

        /// <summary>
        /// Gets or sets the RowSet parsed from the response
        /// </summary>
        public RowSet RowSet { get; set; }

        public Guid? TraceId { get; private set; }
        
        public RowSetMetadata ResultRowsMetadata { get; }

        internal OutputRows(FrameReader reader, ResultMetadata resultMetadata, Guid? traceId)
        {
            ResultRowsMetadata = new RowSetMetadata(reader);
            _rowLength = reader.ReadInt32();
            TraceId = traceId;
            RowSet = new RowSet();
            ProcessRows(RowSet, reader, resultMetadata);
        }

        /// <summary>
        /// Process rows and sets the paging event handler
        /// </summary>
        internal void ProcessRows(RowSet rs, FrameReader reader, ResultMetadata providedResultMetadata)
        {
            RowSetMetadata resultMetadata = null;

            // result metadata in the response takes precedence over the previously provided result metadata.
            if (ResultRowsMetadata != null)
            {
                resultMetadata = ResultRowsMetadata;
                rs.Columns = ResultRowsMetadata.Columns;
                rs.PagingState = ResultRowsMetadata.PagingState;
            }

            // if the response has no column definitions, then SKIP_METADATA was set by the driver
            // the driver only sets this flag for bound statements
            if (resultMetadata?.Columns == null)
            {
                resultMetadata = providedResultMetadata?.RowSetMetadata;
                rs.Columns = resultMetadata?.Columns;
            }

            for (var i = 0; i < _rowLength; i++)
            {
                rs.AddRow(ProcessRowItem(reader, resultMetadata));
            }
        }

        internal virtual Row ProcessRowItem(FrameReader reader, RowSetMetadata resultMetadata)
        {
            var rowValues = new object[resultMetadata.Columns.Length];
            for (var i = 0; i < resultMetadata.Columns.Length; i++)
            {
                var c = resultMetadata.Columns[i];
                var length = reader.ReadInt32();
                if (length < 0)
                {
                    rowValues[i] = null;
                    continue;
                }
                var buffer = GetBuffer(length, c.TypeCode);
                rowValues[i] = reader.ReadFromBytes(buffer, 0, length, c.TypeCode, c.TypeInfo);
            }

            return new Row(rowValues, resultMetadata.Columns, resultMetadata.ColumnIndexes);
        }

        /// <summary>
        /// Reduces allocations by reusing a 16-length buffer for types where is possible
        /// </summary>
        private static byte[] GetBuffer(int length, ColumnTypeCode typeCode)
        {
            if (length > ReusableBufferLength)
            {
                return new byte[length];
            }
            switch (typeCode)
            {
                //blob requires a new instance
                case ColumnTypeCode.Blob:
                case ColumnTypeCode.Inet:
                case ColumnTypeCode.Custom:
                case ColumnTypeCode.Decimal:
                    return new byte[length];
            }
            return ReusableBuffer.Value;
        }

        public void Dispose()
        {

        }
    }
}
