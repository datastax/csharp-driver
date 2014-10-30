using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.Tests
{
    internal static class TestHelper
    {
        public static Row CreateRow(IDictionary<string, object> valueMap)
        {
            var columns = new List<CqlColumn>();
            var rowValues = new List<byte[]>();
            foreach (var kv in valueMap)
            {
                if (kv.Value != null)
                {
                    IColumnInfo typeInfo;
                    var typeCode = TypeCodec.GetColumnTypeCodeInfo(kv.Value.GetType(), out typeInfo);
                    columns.Add(new CqlColumn() { Name = kv.Key, TypeCode = typeCode, TypeInfo = typeInfo });
                }
                else
                {
                    columns.Add(new CqlColumn() { Name = kv.Key, TypeCode = ColumnTypeCode.Text });
                }
                rowValues.Add(TypeCodec.Encode(2, kv.Value));
            }
            var i = 0;
            return new Row(2, rowValues.ToArray(), columns.ToArray(), valueMap.ToDictionary(kv => kv.Key, kv => i++));
        }

        public static IEnumerable<Row> CreateRows(IEnumerable<Dictionary<string, object>> valueMapList)
        {
            return valueMapList.Select(CreateRow);
        }

        public static Host CreateHost(string address, string dc = "dc1", string rack = "rack1")
        {
            var h = new Host(IPAddress.Parse(address), new ConstantReconnectionPolicy(1));
            h.SetLocationInfo(dc, rack);
            return h;
        }

        public static byte GetLastAddressByte(Host h)
        {
            return h.Address.GetAddressBytes()[3];
        }
    }
}
