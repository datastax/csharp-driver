//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

namespace Dse.Serialization.Primitive
{
    internal class IpAddressSerializer : TypeSerializer<IPAddress>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Inet; }
        }

        public override IPAddress Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            if (length == 4 || length == 16)
            {
                return new IPAddress(Utils.FromOffset(buffer, offset, length));
            }
            throw new DriverInternalError("Invalid length of Inet address: " + length);
        }

        public override byte[] Serialize(ushort protocolVersion, IPAddress value)
        {
            return value.GetAddressBytes();
        }
    }
}
