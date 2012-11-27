using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Net;
using System.Globalization;
using System.Threading;


namespace Cassandra.Native
{
    internal class AtomicValue<T>
    {
        T val;
        public AtomicValue(T val)
        {
            this.val = val;
            Thread.MemoryBarrier();
        }
        public T Value
        {
            get
            {
                //lock (this)
                {
                    Thread.MemoryBarrier();
                    var r = this.val;
                    Thread.MemoryBarrier();
                    return r;
                }
            }
            set
            {
                //lock (this)
                {
                    Thread.MemoryBarrier();
                    this.val = value;
                    Thread.MemoryBarrier();
                }
            }
        }
    }

    internal class AtomicArray<T>
    {
        T[] arr = null;
        public AtomicArray(int size)
        {
            arr = new T[size];
            Thread.MemoryBarrier();
        }
        public T this[int idx]
        {
            get
            {
                //lock (this)
                {
                    Thread.MemoryBarrier();
                    var r = this.arr[idx];
                    Thread.MemoryBarrier();
                    return r;
                }
            }
            set
            {
                //lock (this)
                {
                    Thread.MemoryBarrier();
                    arr[idx] = value;
                    Thread.MemoryBarrier();
                }
            }
        }
    }

    internal class Guarded<T>
    {
        T val;

        void AssureLocked()
        {
            if (Monitor.TryEnter(this))
                Monitor.Exit(this);
            else
                throw new System.Threading.SynchronizationLockException();
        }
        
        public Guarded(T val)
        {
            this.val = val;
            Thread.MemoryBarrier();
        }
        public T Value
        {
            get
            {
                AssureLocked();
                return val;
            }
            set
            {
                AssureLocked();
                val = value;
            }
        }
    }

    internal class WeakReference<T> : WeakReference
    {
        public WeakReference(T val): base(val){}
        public T Value { get { return (T)this.Target; } set { this.Target = value; } }
    }


    internal static class StaticRandom
    {
        [ThreadStatic]
        static Random rnd = null;
        public static Random Instance
        {
            get
            {
                if (rnd == null)
                    rnd = new Random(BitConverter.ToInt32(new Guid().ToByteArray(), 0));
                return rnd;
            }
        }
    }

    internal static class IPEndPointParser
    {
        public static IPEndPoint ParseEndpoint(string endPoint)
        {
            string[] ep = endPoint.Split(':');
            if (ep.Length < 2) throw new FormatException("Invalid endpoint format");
            IPAddress ip;
            if (ep.Length > 2)
            {
                if (!IPAddress.TryParse(string.Join(":", ep, 0, ep.Length - 1), out ip))
                {
                    throw new FormatException("Invalid ip-adress");
                }
            }
            else
            {
                if (!IPAddress.TryParse(ep[0], out ip))
                {
                    throw new FormatException("Invalid ip-adress");
                }
            }
            int port;
            if (!int.TryParse(ep[ep.Length - 1], NumberStyles.None, NumberFormatInfo.CurrentInfo, out port))
            {
                throw new FormatException("Invalid port");
            }
            return new IPEndPoint(ip, port);
        }
    }

    internal static class CqlQueryTools
    {
        static Regex IdentifierRx = new Regex(@"\b[a-z][a-z0-9_]*\b", RegexOptions.Compiled);
        public static string CqlIdentifier(string id)
        {
            if (!string.IsNullOrEmpty(id))
            {
                if (!IdentifierRx.IsMatch(id))
                {
                    return "\"" + id.Replace("\"", "\"\"") + "\"";
                }
                else
                {
                    return id;
                }
            }
            throw new ArgumentException("invalid identifier");
        }

        private static readonly string[] HexStringTable = new string[]
{
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
    "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
    "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
    "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
    "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
    "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
    "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
};
        public static string ToHex(byte[] value)
        {
            StringBuilder stringBuilder = new StringBuilder();
            if (value != null)
            {
                foreach (byte b in value)
                {
                    stringBuilder.Append(HexStringTable[b]);
                }
            }

            return stringBuilder.ToString();
        }
    }

    internal static class Utils
    {
        public static bool ArrEqual(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
                return false;

            if (a1.Length != a2.Length)
                return false;

            EqualityComparer<byte> comparer = EqualityComparer<byte>.Default;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!comparer.Equals(a1[i], a2[i])) return false;
            }
            return true;
        }

        public static SortedDictionary<string, int?> ConvertStringToMap(string source)
        {
            var elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":",":").Split(',');
            SortedDictionary<string,int?> map = new SortedDictionary<string,int?>();
            
            foreach (var elem in elements)
            {
                int value;
                if (int.TryParse(elem.Split(':')[1].Replace("\"",""), out value))
                    map.Add(elem.Split(':')[0], value);
                else
                    map.Add(elem.Split(':')[0], null);
            }

            return map;
        }
    }    
}


