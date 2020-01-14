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
using Cassandra.Geometry;

namespace Cassandra.Serialization.Geometry
{
    /// <summary>
    /// A <see cref="LineString"/> type serializer.
    /// </summary>
    internal class LineStringSerializer : GeometrySerializer<LineString>
    {
        private readonly IColumnInfo _typeInfo = new CustomColumnInfo("org.apache.cassandra.db.marshal.LineStringType");

        public override LineString Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }
            if (length < 9)
            {
                throw new ArgumentException("A LineString buffer should contain at least 9 bytes");
            }
            var isLe = IsLittleEndian(buffer, offset);
            offset++;
            var type = (GeometryType) EndianBitConverter.ToInt32(isLe, buffer, offset);
            if (type != GeometryType.LineString)
            {
                throw new ArgumentException("Binary representation was not a LineString");
            }
            offset += 4;
            var pointsLength = EndianBitConverter.ToInt32(isLe, buffer, offset);
            offset += 4;
            if (buffer.Length < offset + pointsLength * 16)
            {
                throw new ArgumentException(string.Format("Buffer length does not match: {0} < {1}", buffer.Length, offset + pointsLength * 16));
            }
            var points = new Point[pointsLength];
            for (var i = 0; i < pointsLength; i++) 
            {
                points[i] = new Point(
                    EndianBitConverter.ToDouble(isLe, buffer, offset),
                    EndianBitConverter.ToDouble(isLe, buffer, offset + 8));
                offset += 16;
            }
            return new LineString(points);
        }

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        public override byte[] Serialize(ushort protocolVersion, LineString value)
        {
            var buffer = new byte[9 + value.Points.Count * 16];
            var isLittleEndian = UseLittleEndianSerialization();
            buffer[0] = isLittleEndian ? (byte)1 : (byte)0;
            var offset = 1;
            EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, (int)GeometryType.LineString);
            offset += 4;
            EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, value.Points.Count);
            offset += 4;
            foreach (var point in value.Points)
            {
                EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, point.X);
                EndianBitConverter.SetBytes(isLittleEndian, buffer, offset + 8, point.Y);
                offset += 16;
            }
            return buffer;
        }
    }
}
