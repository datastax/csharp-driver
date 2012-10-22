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
                Thread.MemoryBarrier();
                var r = this.val;
                Thread.MemoryBarrier();
                return r;
            }
            set
            {
                Thread.MemoryBarrier();
                this.val = value;
                Thread.MemoryBarrier();
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
                Thread.MemoryBarrier();
                var r = this.arr[idx];
                Thread.MemoryBarrier();
                return r;
            }
            set
            {
                Thread.MemoryBarrier();
                arr[idx] = value;
                Thread.MemoryBarrier();
            }
        }
    }

    internal class Guarded<T>
    {
        T val;
        public Guarded(T val)
        {
            this.val = val;
        }
        public T Value { get { return val; } set { val = value; } }
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

    public static class CqlQueryTools
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
    }
}


