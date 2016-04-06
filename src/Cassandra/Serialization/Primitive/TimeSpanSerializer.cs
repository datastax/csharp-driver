using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization.Primitive
{
    internal class TimeSpanSerializer : TypeSerializer<TimeSpan>
    {
        public override ColumnTypeCode CqlType
        {
            get
            {
                return ColumnTypeCode.TimeSpan;
            }
        }

        public override byte[] Serialize(ushort protocolVersion, TimeSpan value)
        {
           return BeConverter.GetBytes((long)value.Ticks);
        }

        public override TimeSpan Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            long ticks = BeConverter.ToInt64(buffer);
            TimeSpan span = new TimeSpan(ticks);
            return span;
        }
    }
}
