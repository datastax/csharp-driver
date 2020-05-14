//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

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

            [System.Diagnostics.CodeAnalysis.SuppressMessage(
                "Security", 
                "CA5351:Do Not Use Broken Cryptographic Algorithms", 
                Justification = "Support for Cassandra's RandomPartitioner")]
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
