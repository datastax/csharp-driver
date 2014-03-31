using System;

namespace Cassandra
{
    internal static class StaticRandom
    {
        [ThreadStatic] private static Random _rnd;

        public static Random Instance
        {
            get { return _rnd ?? (_rnd = new Random(BitConverter.ToInt32(new Guid().ToByteArray(), 0))); }
        }
    }
}