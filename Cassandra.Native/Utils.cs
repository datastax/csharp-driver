using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Globalization;

namespace Cassandra.Native
{
    internal class Locked<T>
    {
        T val;
        public Locked(T val)
        {
            this.val = val;
        }
        public T Value { get { lock (this) return val; } set { lock (this) val = value; } }
    }

    internal class Wrapper<T>
    {
        T val;
        public Wrapper(T val)
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
}


