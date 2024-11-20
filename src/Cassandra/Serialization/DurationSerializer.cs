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

namespace Cassandra.Serialization
{
    internal class DurationSerializer : TypeSerializer<Duration>
    {
        private static readonly ThreadLocal<byte[][]> EncodingBuffers = 
            new ThreadLocal<byte[][]>(() => new [] { new byte[9] , new byte[9], new byte[9]});

        public DurationSerializer(bool asCustomType)
        {
            if (!asCustomType)
            {
                throw new NotSupportedException("Duration type is only supported as custom type (protocol v4)");
            }
        }

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return new CustomColumnInfo("org.apache.cassandra.db.marshal.DurationType"); }
        }

        public override Duration Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var months = (int)VintSerializer.ReadVInt(buffer, ref offset);
            var days = (int)VintSerializer.ReadVInt(buffer, ref offset);
            var nanoseconds = VintSerializer.ReadVInt(buffer, ref offset);
            return new Duration(months, days, nanoseconds);
        }

        public override byte[] Serialize(ushort protocolVersion, Duration value)
        {
            var lengthMonths = VintSerializer.WriteVInt(value.Months, EncodingBuffers.Value[0]);
            var lengthDays = VintSerializer.WriteVInt(value.Days, EncodingBuffers.Value[1]);
            var lengthNanos = VintSerializer.WriteVInt(value.Nanoseconds, EncodingBuffers.Value[2]);
            // Can be improved once the buffer pool is made available for serializers
            var buffer = new byte[lengthMonths + lengthDays + lengthNanos];
            var offset = 0;
            Buffer.BlockCopy(EncodingBuffers.Value[0], 0, buffer, offset, lengthMonths);
            offset += lengthMonths;
            Buffer.BlockCopy(EncodingBuffers.Value[1], 0, buffer, offset, lengthDays);
            offset += lengthDays;
            Buffer.BlockCopy(EncodingBuffers.Value[2], 0, buffer, offset, lengthNanos);
            return buffer;
        }
    }
}
