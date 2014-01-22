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
    internal class ExecuteRequest : IRequest
    {
        public const byte OpCode = 0x0A;

        readonly int _streamId;
        readonly object[] _values;
        readonly byte[] _id;
        readonly RowSetMetadata _metadata;
        readonly ConsistencyLevel _consistency;
        private readonly byte _flags = 0x00;

        public ExecuteRequest(int streamId, byte[] id, RowSetMetadata metadata, object[] values, ConsistencyLevel consistency, bool tracingEnabled)
        {
            if (values.Length != metadata.Columns.Length)
                throw new System.ArgumentException("Number of values does not match with number of prepared statement markers(?).", "values");            

            this._streamId = streamId;
            this._values = values;
            this._id = id;
            this._metadata = metadata;
            this._consistency = consistency;
            if (tracingEnabled)
                this._flags = 0x02;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(RequestFrame.ProtocolRequestVersionByte, _flags, (byte)_streamId, OpCode);
            wb.WriteShortBytes(_id);
            wb.WriteUInt16((ushort) _values.Length);
            for (int i = 0; i < _metadata.Columns.Length; i++)
            {
                var bytes = _metadata.ConvertFromObject(i, _values[i]);
                wb.WriteBytes(bytes);
            }
            wb.WriteInt16((short)_consistency);
            return wb.GetFrame();
        }
    }
}
