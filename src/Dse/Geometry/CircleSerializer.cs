using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Cassandra.Serialization;

namespace Dse.Geometry
{
    /// <summary>
    /// A <see cref="Circle"/> type serializer.
    /// </summary>
    internal class CircleSerializer : GeometrySerializer<Circle>
    {
        private readonly IColumnInfo _typeInfo = new CustomColumnInfo("org.apache.cassandra.db.marshal.CircleType");

        public override Circle Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }
            if (length != 29)
            {
                throw new ArgumentException("A Circle buffer should contain 29 bytes");
            }
            var isLe = IsLittleEndian(buffer, offset);
            var type = (GeometryType) EndianBitConverter.ToInt32(isLe, buffer, offset + 1);
            if (type != GeometryType.Circle)
            {
                throw new ArgumentException("Binary representation was not a Circle");
            }
            var center = new Point(EndianBitConverter.ToDouble(isLe, buffer, offset + 5), EndianBitConverter.ToDouble(isLe, buffer, offset + 13));
            return new Circle(center, EndianBitConverter.ToDouble(isLe, buffer, offset + 21));
        }

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        public override byte[] Serialize(ushort protocolVersion, Circle value)
        {
            var buffer = new byte[29];
            var isLe = IsCpuLittleEndian();
            buffer[0] = isLe ? (byte)1 : (byte)0;
            EndianBitConverter.SetBytes(isLe, buffer, 1, (int)GeometryType.Circle);
            EndianBitConverter.SetBytes(isLe, buffer, 5, value.Center.X);
            EndianBitConverter.SetBytes(isLe, buffer, 13, value.Center.Y);
            EndianBitConverter.SetBytes(isLe, buffer, 21, value.Radius);
            return buffer;
        }
    }
}
