//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Numerics;
using System.Security.Cryptography;

namespace Dse
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

        public override string ToString()
        {
            return _value.ToString();
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
                if (_md5 == null) 
                    _md5 = MD5.Create();
                
                var hash = _md5.ComputeHash(partitionKey);
                
                var reversedHash = new byte[hash.Length];
                for(int x = hash.Length - 1, y = 0; x >= 0; --x, ++y)
                {
                    reversedHash[y] = hash[x];
                }
                var bigInteger = BigInteger.Abs(new BigInteger(reversedHash));
                
                return new RPToken(bigInteger);
            }
        }
    }
}
