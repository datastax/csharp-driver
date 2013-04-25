using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.Diagnostics;


namespace Cassandra
{
    internal class AtomicValue<T>
    {
        T _val;
        public AtomicValue(T val)
        {
            this._val = val;
            Thread.MemoryBarrier();
        }
        public T Value
        {
            get
            {
                    Thread.MemoryBarrier();
                    var r = this._val;
                    Thread.MemoryBarrier();
                    return r;
            }
            set
            {
                    Thread.MemoryBarrier();
                    this._val = value;
                    Thread.MemoryBarrier();
            }
        }
    }

    internal class AtomicArray<T>
    {
        readonly T[] _arr = null;
        public AtomicArray(int size)
        {
            _arr = new T[size];
            Thread.MemoryBarrier();
        }
        public T this[int idx]
        {
            get
            {
                    Thread.MemoryBarrier();
                    var r = this._arr[idx];
                    Thread.MemoryBarrier();
                    return r;
            }
            set
            {
                    Thread.MemoryBarrier();
                    _arr[idx] = value;
                    Thread.MemoryBarrier();
            }
        }
    }

    internal class Guarded<T>
    {
        T _val;

        void AssureLocked()
        {
            if (Monitor.TryEnter(this))
                Monitor.Exit(this);
            else
                throw new System.Threading.SynchronizationLockException();
        }
        
        public Guarded(T val)
        {
            this._val = val;
            Thread.MemoryBarrier();
        }
        public T Value
        {
            get
            {
                AssureLocked();
                return _val;
            }
            set
            {
                AssureLocked();
                _val = value;
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
        static Random _rnd = null;
        public static Random Instance
        {
            get { return _rnd ?? (_rnd = new Random(BitConverter.ToInt32(new Guid().ToByteArray(), 0))); }
        }
    }

    internal static class CqlQueryTools
    {
        static readonly Regex IdentifierRx = new Regex(@"\b[a-z][a-z0-9_]*\b", RegexOptions.Compiled);
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
            var stringBuilder = new StringBuilder();
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
        public static long GetTimestampFromGuid(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();
            bytes[7] &= (byte)0x0f;
            return BitConverter.ToInt64(bytes, 0);
        }

        public static string ConvertToCqlMap(IDictionary<string, string> source)
        {
            StringBuilder sb = new StringBuilder("{");
            if (source.Count > 0)
            {
                int counter = 0;
                foreach (var elem in source)
                {
                    counter++;
                    sb.Append("'" + elem.Key + "'" + " : " + "'"+elem.Value+"'"  + ((source.Count != counter) ? ", " : "}"));
                    //sb.Append("'" + elem.Key + "'" + " : " + (elem.Key == "class" ? "'" + elem.Value + "'" : elem.Value) + ((source.Count != counter) ? ", " : "}"));
                }
            }
            else sb.Append("}");

            return sb.ToString();
        }

        public static IDictionary<string, string> ConvertStringToMap(string source)
        {
            var elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":", ":").Split(',');
            var map = new SortedDictionary<string, string>();

            if (source != "{}")
                foreach (var elem in elements)
                    map.Add(elem.Split(':')[0].Replace("\"", ""), elem.Split(':')[1].Replace("\"", ""));
                
            return map;
        }
        
        public static IDictionary<string, int> ConvertStringToMapInt(string source)
        {
            var elements = source.Replace("{\"", "").Replace("\"}", "").Replace("\"\"", "\"").Replace("\":",":").Split(',');
            var map = new SortedDictionary<string,int>();

            if(source != "{}")
                foreach (var elem in elements)
                {
                    int value;
                    if (int.TryParse(elem.Split(':')[1].Replace("\"", ""), out value))
                        map.Add(elem.Split(':')[0].Replace("\"", ""), value);
                    else
                        throw new FormatException("Value of keyspace strategy option is in invalid format!");                        
                }

            return map;
        }

        public static IEnumerable<IPAddress> ResolveHostByName(string address)
        {
            IPAddress addr;
            if (IPAddress.TryParse(address, out addr))
            {
                return new List<IPAddress>() { addr };
            }
            else
            {
                var hst = Dns.GetHostEntry(address);
                return hst.AddressList;
            }
        }

        public static bool CompareIDictionary<TKey, TValue>(IDictionary<TKey, TValue> dict1, IDictionary<TKey, TValue> dict2)
        {
            if (dict1 == dict2) return true;
            if ((dict1 == null) || (dict2 == null)) return false;
            if (dict1.Count != dict2.Count) return false;

            var comp = EqualityComparer<TValue>.Default;

            foreach (KeyValuePair<TKey, TValue> kvp in dict1)
            {
                TValue value2;
                if (!dict2.TryGetValue(kvp.Key, out value2))
                    return false;
                if (!comp.Equals(kvp.Value, value2))
                    return false;
            }
            return true;
        }

    }

    public class ReadOnlyDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        //based on http://www.blackwasp.co.uk/ReadOnlyDictionary.aspx

        readonly IDictionary<TKey, TValue> _dictionary;

        public ReadOnlyDictionary()
        {
            _dictionary = new Dictionary<TKey, TValue>();
        }

        public ReadOnlyDictionary(ReadOnlyDictionary<TKey, TValue> rodic)
        {
            _dictionary = rodic._dictionary;
        }

        public ReadOnlyDictionary(IDictionary<TKey, TValue> dictionary)
        {
            _dictionary = dictionary;
        }

        #region IDictionary<TKey,TValue> Members

        void IDictionary<TKey, TValue>.Add(TKey key, TValue value)
        {
            throw ReadOnlyException();
        }

        public bool ContainsKey(TKey key)
        {
            return _dictionary.ContainsKey(key);
        }

        public ICollection<TKey> Keys
        {
            get { return _dictionary.Keys; }
        }

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            throw ReadOnlyException();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dictionary.TryGetValue(key, out value);
        }

        public ICollection<TValue> Values
        {
            get { return _dictionary.Values; }
        }

        public TValue this[TKey key]
        {
            get
            {
                Thread.MemoryBarrier();
                return _dictionary[key];
            }
        }

        TValue IDictionary<TKey, TValue>.this[TKey key]
        {
            get
            {
                return this[key];
            }
            set
            {
                throw ReadOnlyException();
            }
        }

        #endregion

        #region ICollection<KeyValuePair<TKey,TValue>> Members

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            throw ReadOnlyException();
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Clear()
        {
            throw ReadOnlyException();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _dictionary.Contains(item);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            _dictionary.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _dictionary.Count; }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            throw ReadOnlyException();
        }

        #endregion

        #region IEnumerable<KeyValuePair<TKey,TValue>> Members

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dictionary.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private static Exception ReadOnlyException()
        {
            return new NotSupportedException("This dictionary is read-only");
        }

        internal void InternalSetup(TKey key, TValue value)
        {
            Thread.MemoryBarrier();
            if (!_dictionary.ContainsKey(key))
                _dictionary.Add(key, value);
            else
                _dictionary[key] = value;
            Thread.MemoryBarrier();
        }
    }

    public class Logger
    {                        
        private string category;
        private StringBuilder sb = null;
         
        public Logger(Type type)
        {            
            category = type.FullName;
        }

        private string printStackTrace()
        {
            StringBuilder sb = new StringBuilder();            
            foreach (var frame in new StackTrace(3, true).GetFrames()) // skipping 3 frames from logger class. 
                sb.Append(frame);
            return sb.ToString(); 
        }

        private string getExceptionAndAllInnerEx(Exception ex, bool recur = false)
        {
            if(!recur || sb == null)
                sb = new StringBuilder();
            sb.Append(string.Format("( Exception! Source {0} \n Message: {1} \n StackTrace:\n {2} ", ex.Source, ex.Message,
                (Diagnostics.CassandraStackTraceIncluded ? (recur ? ex.StackTrace : printStackTrace()) : "To display StackTrace, change Debugging.StackTraceIncluded property value to true."), this.category));
            if (ex.InnerException != null)
                getExceptionAndAllInnerEx(ex.InnerException, true);            
            
            sb.Append(")");
            return sb.ToString();
        }

        private readonly string DateFormat = "MM/dd/yyyy H:mm:ss.fff zzz";

        public void Error(Exception ex)
        {
            if (ex != null) //shouldn't happen
            {
                if (Diagnostics.CassandraTraceSwitch.TraceError)
                    Trace.WriteLine(string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), getExceptionAndAllInnerEx(ex)), category);
            }
            else
                throw new InvalidOperationException();
        }

        public void Error(string msg, Exception ex = null)
        {
            if (Diagnostics.CassandraTraceSwitch.TraceError)
                Trace.WriteLine(string.Format("{0} #ERROR: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg + (ex != null ? "\nEXCEPTION:\n " + getExceptionAndAllInnerEx(ex) : String.Empty)), category);
        }
        
        public void Warning(string msg)
        {
            if(Diagnostics.CassandraTraceSwitch.TraceWarning)
                Trace.WriteLine(string.Format("{0} #WARNING: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }

        public void Info(string msg)
        {            
            if (Diagnostics.CassandraTraceSwitch.TraceInfo)
                Trace.WriteLine(string.Format("{0} #INFO: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }
        
        public void Verbose(string msg)
        {            
            if(Diagnostics.CassandraTraceSwitch.TraceVerbose)
                Trace.WriteLine(string.Format("{0} #VERBOSE: {1}", DateTimeOffset.Now.DateTime.ToString(DateFormat), msg), category);
        }
    }

}
