using System;
#if NET_40_OR_GREATER
using System.Numerics;
using System.Security.Cryptography;
#endif

namespace Cassandra
{
    // We really only use the generic for type safety and it's not an interface because we don't want to expose
    // Note: we may want to expose this later if people use custom partitioner and want to be able to extend that. This is way premature however.
    abstract class TokenFactory
    {
        public static TokenFactory GetFactory(string partitionerName)
        {
            if (partitionerName.EndsWith("Murmur3Partitioner"))
                return M3PToken.Factory;
#if NET_40_OR_GREATER
            else if (partitionerName.EndsWith("RandomPartitioner"))
                return RPToken.Factory;
#endif
            else if (partitionerName.EndsWith("OrderedPartitioner"))
                return OPPToken.Factory;
            else
                return null;
        }

        public abstract IToken Parse(String tokenStr);
        public abstract IToken Hash(byte[] partitionKey);
    }

    interface IToken : IComparable
    {
    }

    // Murmur3Partitioner tokens
    class M3PToken : IToken
    {
        private readonly long _value;

        class M3PTokenFactory : TokenFactory
        {
            public override IToken Parse(string tokenStr)
            {
                return new M3PToken(long.Parse(tokenStr));
            }

            public override IToken Hash(byte[] partitionKey)
            {
                long v = (long)MurmurHash.Hash3_x64_128(partitionKey, 0, partitionKey.Length, 0)[0];
                return new M3PToken(v == long.MinValue ? long.MaxValue : v);
            }
        }

        public static readonly TokenFactory Factory = new M3PTokenFactory();

        private M3PToken(long value)
        {
            this._value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return _value == ((M3PToken)obj)._value;
        }

        public override int GetHashCode()
        {
            return (int)(_value ^ ((long)((ulong)_value >> 32)));
        }

        public int CompareTo(object obj)
        {
            var other = obj as M3PToken;
            long otherValue = other._value;
            return _value < otherValue ? -1 : (_value == otherValue) ? 0 : 1;
        }
    }

    // OPPartitioner tokens
    class OPPToken : IToken
    {
        private readonly byte[] _value;

        class OPPTokenFactory : TokenFactory
        {
            public override IToken Parse(string tokenStr)
            {
                return new OPPToken(System.Text.Encoding.UTF8.GetBytes(tokenStr));
            }

            public override IToken Hash(byte[] partitionKey)
            {
                return new OPPToken(partitionKey);
            }
        }

        public static readonly TokenFactory Factory = new OPPTokenFactory();


        private OPPToken(byte[] value)
        {
            this._value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return _value == ((OPPToken)obj)._value;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as OPPToken;
            for (int i = 0; i < _value.Length && i < other._value.Length; i++)
            {
                int a = (_value[i] & 0xff);
                int b = (other._value[i] & 0xff);
                if (a != b)
                    return a - b;
            }
            return 0;
        }
    }

#if NET_40_OR_GREATER

    // RandomPartitioner tokens
    class RPToken : IToken
    {
        private readonly BigInteger _value;

        class RPTokenFactory : TokenFactory
        {
            public override IToken Parse(string tokenStr)
            {
                return new RPToken(BigInteger.Parse(tokenStr));
            }

            [ThreadStatic]
            static MD5 _md5 = null;
            public override IToken Hash(byte[] partitionKey)
            {
                if (_md5 == null) _md5 = MD5.Create();
                return new RPToken(new BigInteger(_md5.ComputeHash(partitionKey)));
            }
        }

        public static readonly TokenFactory Factory = new RPTokenFactory();


        private RPToken(BigInteger value)
        {
            this._value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return _value == ((RPToken)obj)._value;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as RPToken;
            return _value.CompareTo(other._value);
        }
    }

#endif
}
