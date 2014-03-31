using System;
using System.Numerics;
using System.Security.Cryptography;

namespace Cassandra
{
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
}