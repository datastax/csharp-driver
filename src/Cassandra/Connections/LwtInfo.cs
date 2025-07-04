using System;
using System.Collections.Generic;

namespace Cassandra.Connections
{
    public class LwtInfo
    {
        private const string SCYLLA_LWT_ADD_METADATA_MARK_KEY = "SCYLLA_LWT_ADD_METADATA_MARK";
        private const string LWT_OPTIMIZATION_META_BIT_MASK_KEY = "LWT_OPTIMIZATION_META_BIT_MASK";

        private readonly int mask;

        private LwtInfo(int mask)
        {
            this.mask = mask;
        }

        public int GetMask()
        {
            return mask;
        }

        public bool IsLwt(int flags)
        {
            return (flags & mask) == mask;
        }

        public static LwtInfo ParseLwtInfo(IDictionary<string, string[]> supported)
        {
            if (!supported.TryGetValue(SCYLLA_LWT_ADD_METADATA_MARK_KEY, out var list))
            {
                return null;
            }
            if (list == null || list.Length != 1)
            {
                return null;
            }
            var val = list[0];
            if (val == null || !val.StartsWith(LWT_OPTIMIZATION_META_BIT_MASK_KEY + "="))
            {
                return null;
            }
            long parsedMask;
            try
            {
                parsedMask = long.Parse(val.Substring((LWT_OPTIMIZATION_META_BIT_MASK_KEY + "=").Length));
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Error while parsing {LWT_OPTIMIZATION_META_BIT_MASK_KEY}: {e.Message}");
                return null;
            }
            if (parsedMask > int.MaxValue)
            {
                // Server returns mask as unsigned int32, so convert to signed int32
                parsedMask += int.MinValue;
                parsedMask += int.MinValue;
            }
            return new LwtInfo((int)parsedMask);
        }
    }
}
