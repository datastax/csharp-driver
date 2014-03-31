using System;

namespace Cassandra
{
    internal static class StaticRandom
    {
        [ThreadStatic]
        static Random _rnd = null;
        public static Random Instance
        {
            get { return _rnd ?? (_rnd = new Random(BitConverter.ToInt32(new Guid().ToByteArray(), 0))); }
        }
    }
}