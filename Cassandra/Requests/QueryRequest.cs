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
     internal class QueryRequest : IBatchableRequest
    {
        public const byte OpCode = 0x07;

        private readonly int _streamId;
        private readonly string _cqlQuery;
        readonly object[] _values;
        private readonly ConsistencyLevel _consistency;
        private readonly byte _flags = 0x00;

        public QueryRequest(int streamId, string cqlQuery, object[] values, ConsistencyLevel consistency, bool tracingEnabled)
        {
            this._streamId = streamId;
            this._values = values;
            this._cqlQuery = cqlQuery;
            this._consistency = consistency;
            if (tracingEnabled)
                this._flags = 0x02;
        }

        public RequestFrame GetFrame()
        {
            var wb = new BEBinaryWriter();
            wb.WriteFrameHeader(RequestFrame.ProtocolRequestVersionByte, _flags, (byte)_streamId, OpCode);
            wb.WriteLongString(_cqlQuery);
            wb.WriteInt16((short)_consistency);
            if (_values == null || _values.Length == 0)
                wb.WriteByte(0x0); //query flags
            else
            {
                wb.WriteByte(0x01);//flags Values
                wb.WriteUInt16((ushort)_values.Length);
                for (int i = 0; i < _values.Length; i++)
                {
                    var bytes = TypeInterpreter.InvCqlConvert(_values[i]);
                    wb.WriteBytes(bytes);
                } 
            }
            return wb.GetFrame();
        }

        public void WriteToBatch(BEBinaryWriter wb)
        {
            wb.WriteByte(0);//not a prepared query
            wb.WriteLongString(_cqlQuery);
            if (_values == null || _values.Length == 0)
                wb.WriteInt16(0);//not values
            else
            {
                wb.WriteUInt16((ushort)_values.Length);
                for (int i = 0; i < _values.Length; i++)
                {
                    var bytes = TypeInterpreter.InvCqlConvert(_values[i]);
                    wb.WriteBytes(bytes);
                }
            }
        }
    }
}
