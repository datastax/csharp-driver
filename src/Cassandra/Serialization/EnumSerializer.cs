using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization
{
    internal class EnumSerializer : TypeSerializer<object>
    {
        public override ColumnTypeCode CqlType
        {
            get
            {
                return ColumnTypeCode.Enum;
            }
        }

        public override object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            string enumText = Encoding.UTF8.GetString(buffer);
            return Enum.Parse(typeInfo.GetType(), enumText, true); // Ignore the case here
        }

        public override byte[] Serialize(ushort protocolVersion, object value)
        {
            // Doing essentially what CheckArgument does but for an enumeration
            if (value == null)
                throw new ArgumentNullException();

            if (value.GetType().IsEnum == false)
            {
                throw new InvalidTypeException("value", value.GetType().FullName, new object[] { value.GetType().FullName });
            }

            return Encoding.UTF8.GetBytes(value.ToString());
        }
    }
}
