﻿//
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

// ReSharper disable CheckNamespace
namespace Cassandra
{
    internal class OutputRows : IOutput
    {
        private readonly int _rowLength;
        private readonly RowSetMetadata _metadata;
        private readonly byte _protocolVersion;
        private static readonly ThreadLocal<byte[]> ReusableBuffer = new ThreadLocal<byte[]>(() => new byte[16]);

        /// <summary>
        /// Gets or sets the RowSet parsed from the response
        /// </summary>
        public RowSet RowSet { get; set; }

        public Guid? TraceId { get; private set; }

        internal OutputRows(byte protocolVersion, FrameReader reader, Guid? traceId)
        {
            _protocolVersion = protocolVersion;
            _metadata = new RowSetMetadata(reader);
            _rowLength = reader.ReadInt32();
            TraceId = traceId;
            RowSet = new RowSet();
            ProcessRows(RowSet, reader);
        }

        /// <summary>
        /// Process rows and sets the paging event handler
        /// </summary>
        internal void ProcessRows(RowSet rs, FrameReader reader)
        {
            if (_metadata != null)
            {
                rs.Columns = _metadata.Columns;
                rs.PagingState = _metadata.PagingState;
            }
            for (var i = 0; i < _rowLength; i++)
            {
                rs.AddRow(ProcessRowItem(reader));
            }
        }

        internal virtual Row ProcessRowItem(FrameReader reader)
        {
            var rowValues = new object[_metadata.Columns.Length];
            for (var i = 0; i < _metadata.Columns.Length; i++)
            {
                var c = _metadata.Columns[i];
                var length = reader.ReadInt32();
                if (length < 0)
                {
                    rowValues[i] = null;
                    continue;
                }
                var buffer = GetBuffer(length, c.TypeCode);
                reader.Read(buffer, 0, length);
                rowValues[i] = TypeCodec.Decode(_protocolVersion, buffer, c.TypeCode, c.TypeInfo);
            }

            return new Row(rowValues, _metadata.Columns, _metadata.ColumnIndexes);
        }

        /// <summary>
        /// Reduces allocations by reusing a 16-length buffer for types where is possible
        /// </summary>
        private static byte[] GetBuffer(int length, ColumnTypeCode typeCode)
        {
            if (length > 16)
            {
                return new byte[length];
            }
            switch (typeCode)
            {
                //blob and inet requires a new instance
                case ColumnTypeCode.Blob:
                case ColumnTypeCode.Inet:
                //just to be safe
                case ColumnTypeCode.Custom:
                //The TypeCodec does not support offset and count for text
                case ColumnTypeCode.Ascii:
                case ColumnTypeCode.Text:
                case ColumnTypeCode.Varchar:
                //The TypeCodec does not support offset and count for varint
                case ColumnTypeCode.Varint:
                //The Decimal converter does not support count for decimal
                case ColumnTypeCode.Decimal:
                //The TypeCodec does not support offset and count for udts
                case ColumnTypeCode.Udt:
                case ColumnTypeCode.Tuple:
                    return new byte[length];
            }
            return ReusableBuffer.Value;
        }

        public void Dispose()
        {

        }
    }
}
