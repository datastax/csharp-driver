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
    /// A <see cref="Point"/> type serializer.
    /// </summary>
    internal class PointSerializer : GeometrySerializer<Point>
    {
        private readonly IColumnInfo _typeInfo = new CustomColumnInfo("org.apache.cassandra.db.marshal.PointType");

        public override Point Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }
            if (length != 21)
            {
                throw new ArgumentException("2D Point buffer should contain 21 bytes");
            }
            var isLe = IsLittleEndian(buffer, offset);
            var type = (GeometryType) EndianBitConverter.ToInt32(isLe, buffer, offset + 1);
            if (type != GeometryType.Point2D)
            {
                throw new ArgumentException("Binary representation was not a point");
            }
            return new Point(
                EndianBitConverter.ToDouble(isLe, buffer, offset + 5), 
                EndianBitConverter.ToDouble(isLe, buffer, offset + 13));
        }

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        public override byte[] Serialize(ushort protocolVersion, Point value)
        {
            var buffer = new byte[21];
            var isLittleEndian = UseLittleEndianSerialization();
            buffer[0] = isLittleEndian ? (byte)1 : (byte)0;
            EndianBitConverter.SetBytes(isLittleEndian, buffer, 1, (int)GeometryType.Point2D);
            EndianBitConverter.SetBytes(isLittleEndian, buffer, 5, value.X);
            EndianBitConverter.SetBytes(isLittleEndian, buffer, 13, value.Y);
            return buffer;
        }
    }
}
