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
using System.Collections.Generic;
using System.Linq;
using Cassandra.Geometry;

namespace Cassandra.Serialization.Geometry
{
    internal class PolygonSerializer : GeometrySerializer<Polygon>
    {
        private readonly IColumnInfo _typeInfo = new CustomColumnInfo("org.apache.cassandra.db.marshal.PolygonType");

        public override Polygon Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }
            if (length < 9)
            {
                throw new ArgumentException("A Polygon buffer should contain at least 9 bytes");
            }
            var isLe = IsLittleEndian(buffer, offset);
            offset++;
            var type = (GeometryType)EndianBitConverter.ToInt32(isLe, buffer, offset);
            if (type != GeometryType.Polygon)
            {
                throw new ArgumentException("Binary representation was not a Polygon");
            }
            offset += 4;
            var ringsLength = EndianBitConverter.ToInt32(isLe, buffer, offset);
            offset += 4;
            var ringsArray = new IList<Point>[ringsLength];
            for (var ringIndex = 0; ringIndex < ringsLength; ringIndex++)
            {
                var pointsLength = EndianBitConverter.ToInt32(isLe, buffer, offset);
                offset += 4;
                if (buffer.Length < offset + pointsLength * 16)
                {
                    throw new ArgumentException("Length of the buffer does not match");
                }
                var ring = new Point[pointsLength];
                for (var i = 0; i < pointsLength; i++)
                {
                    ring[i] = new Point(
                        EndianBitConverter.ToDouble(isLe, buffer, offset),
                        EndianBitConverter.ToDouble(isLe, buffer, offset + 8));
                    offset += 16;
                }
                ringsArray[ringIndex] = ring;
            }
            return new Polygon(ringsArray);
        }

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        public override byte[] Serialize(ushort protocolVersion, Polygon value)
        {
            var totalRingsLength = value.Rings.Sum(r => 4 + r.Count*16);
            var buffer = new byte[9 + totalRingsLength];
            var isLittleEndian = UseLittleEndianSerialization();
            buffer[0] = isLittleEndian ? (byte)1 : (byte)0;
            var offset = 1;
            EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, (int)GeometryType.Polygon );
            offset += 4;
            EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, value.Rings.Count);
            offset += 4;
            foreach (var ring in value.Rings)
            {
                EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, ring.Count);
                offset += 4;
                foreach (var point in ring)
                {
                    EndianBitConverter.SetBytes(isLittleEndian, buffer, offset, point.X);
                    EndianBitConverter.SetBytes(isLittleEndian, buffer, offset + 8, point.Y);
                    offset += 16;   
                }
            }
            return buffer;
        }
    }
}
