using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using Cassandra.Native;
using System.Threading;
using System.Data.Common;

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
                return M3PToken.FACTORY;
#if NET_40_OR_GREATER
            else if (partitionerName.EndsWith("RandomPartitioner"))
                return RPToken.FACTORY;
#endif
            else if (partitionerName.EndsWith("OrderedPartitioner"))
                return OPPToken.FACTORY;
            else
                return null;
        }

        public abstract Token Parse(String tokenStr);
        public abstract Token Hash(byte[] partitionKey);
    }

    interface Token : IComparable
    {
    }

    // Murmur3Partitioner tokens
    class M3PToken : Token
    {
        private readonly long value;

        class M3PTokenFactory : TokenFactory
        {
            public override Token Parse(string tokenStr)
            {
                return new M3PToken(long.Parse(tokenStr));
            }

            public override Token Hash(byte[] partitionKey)
            {
                long v = (long)MurmurHash.Hash3_x64_128(partitionKey, 0, partitionKey.Length, 0)[0];
                return new M3PToken(v == long.MinValue ? long.MaxValue : v);
            }
        }

        public static readonly TokenFactory FACTORY = new M3PTokenFactory();

        private M3PToken(long value)
        {
            this.value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return value == ((M3PToken)obj).value;
        }

        public override int GetHashCode()
        {
            return (int)(value ^ ((long)((ulong)value >> 32)));
        }

        public int CompareTo(object obj)
        {
            var other = obj as M3PToken;
            long otherValue = other.value;
            return value < otherValue ? -1 : (value == otherValue) ? 0 : 1;
        }
    }

    // OPPartitioner tokens
    class OPPToken : Token
    {
        private readonly byte[] value;

        class OPPTokenFactory : TokenFactory
        {
            public override Token Parse(string tokenStr)
            {
                return new OPPToken(System.Text.Encoding.UTF8.GetBytes(tokenStr));
            }

            public override Token Hash(byte[] partitionKey)
            {
                return new OPPToken(partitionKey);
            }
        }

        public static readonly TokenFactory FACTORY = new OPPTokenFactory();


        private OPPToken(byte[] value)
        {
            this.value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return value == ((OPPToken)obj).value;
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as OPPToken;
            for (int i = 0; i < value.Length && i < other.value.Length; i++)
            {
                int a = (value[i] & 0xff);
                int b = (other.value[i] & 0xff);
                if (a != b)
                    return a - b;
            }
            return 0;
        }
    }

#if NET_40_OR_GREATER

    // RandomPartitioner tokens
    class RPToken : Token
    {
        private readonly BigInteger value;

        class RPTokenFactory : TokenFactory
        {
            public override Token Parse(string tokenStr)
            {
                return new RPToken(BigInteger.Parse(tokenStr));
            }

            [ThreadStatic]
            static MD5 md5 = null;
            public override Token Hash(byte[] partitionKey)
            {
                if (md5 == null) md5 = MD5.Create();
                return new RPToken(new BigInteger(md5.ComputeHash(partitionKey)));
            }
        }

        public static readonly TokenFactory FACTORY = new RPTokenFactory();


        private RPToken(BigInteger value)
        {
            this.value = value;
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.GetType() != obj.GetType())
                return false;

            return value == ((RPToken)obj).value;
        }

        public override int GetHashCode()
        {
            return value.GetHashCode();
        }

        public int CompareTo(object obj)
        {
            var other = obj as RPToken;
            return value.CompareTo(other.value);
        }
    }

#endif
}
