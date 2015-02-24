using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

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

        public static Host CreateHost(string address, string dc = "dc1", string rack = "rack1", IEnumerable<string> tokens = null)
        {
            var h = new Host(IPAddress.Parse(address), new ConstantReconnectionPolicy(1));
            h.SetLocationInfo(dc, rack);
            h.Tokens = tokens;
            return h;
        }

        public static byte GetLastAddressByte(Host h)
        {
            return GetLastAddressByte(h.Address);
        }

        public static byte GetLastAddressByte(IPAddress address)
        {
            return address.GetAddressBytes()[3];
        }

        /// <summary>
        /// Invokes actions in parallel using 1 thread per action
        /// </summary>
        public static void ParallelInvoke(IEnumerable<Action> actions)
        {   
            var parallelOptions = new ParallelOptions
            {
                TaskScheduler = new ThreadPerTaskScheduler(), 
                MaxDegreeOfParallelism = 1000
            };
            Parallel.Invoke(parallelOptions, actions.ToArray());
        }

        public static byte[] HexToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                             .ToArray();
        }

        /// <summary>
        /// Invokes the same action multiple times in parallel using 1 thread per action
        /// </summary>
        internal static void ParallelInvoke(Action action, int times)
        {
            ParallelInvoke(new List<Action>(Enumerable.Repeat<Action>(action, 100)));
        }

        /// <summary>
        /// Invokes the same action multiple times serially using the current thread
        /// </summary>
        internal static void Invoke(Action action, int times)
        {
            for (var i = 0; i < times; i++)
            {
                action();
            }
        }
    }
}
