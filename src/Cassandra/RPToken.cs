using System;
using System.Numerics;
using System.Security.Cryptography;

namespace Cassandra
{
    internal class RPToken : IToken
    {
        public static readonly TokenFactory Factory = new RPTokenFactory();
        private readonly BigInteger _value;


        private RPToken(BigInteger value)
        {
            _value = value;
        }

        public int CompareTo(object obj)
        {
            var other = obj as RPToken;
            return _value.CompareTo(other._value);
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || GetType() != obj.GetType())
                return false;

            return _value == ((RPToken) obj)._value;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        private class RPTokenFactory : TokenFactory
        {
            [ThreadStatic] private static MD5 _md5;

            public override IToken Parse(string tokenStr)
            {
                return new RPToken(BigInteger.Parse(tokenStr));
            }

            public override IToken Hash(byte[] partitionKey)
            {
                if (_md5 == null) _md5 = MD5.Create();
                return new RPToken(new BigInteger(_md5.ComputeHash(partitionKey)));
            }
        }
    }
}