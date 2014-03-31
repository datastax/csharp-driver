using System;

namespace Cassandra.Data.Linq
{
    public class CqlToken
    {
        public readonly object[] Values;

        internal CqlToken(object[] v)
        {
            Values = v;
        }

        public static CqlToken Create<T>(T v)
        {
            return new CqlToken(new object[] {v});
        }

        public static CqlToken Create<T1, T2>(T1 v1, T2 v2)
        {
            return new CqlToken(new object[] {v1, v2});
        }

        public static CqlToken Create<T1, T2, T3>(T1 v1, T2 v2, T3 v3)
        {
            return new CqlToken(new object[] {v1, v2, v3});
        }

        public static CqlToken Create<T1, T2, T3, T4>(T1 v1, T2 v2, T3 v3, T4 v4)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4});
        }

        public static CqlToken Create<T1, T2, T3, T4, T5>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4, v5});
        }

        public static CqlToken Create<T1, T2, T3, T4, T5, T6>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4, v5, v6});
        }

        public override int GetHashCode()
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public override bool Equals(object obj)
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
    }
}