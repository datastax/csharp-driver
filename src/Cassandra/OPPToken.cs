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

using System.Text;

namespace Cassandra
{
    internal class OPPToken : IToken
    {
        public static readonly TokenFactory Factory = new OPPTokenFactory();
        private readonly byte[] _value;


        private OPPToken(byte[] value)
        {
            _value = value;
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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj))
                return true;
            if (obj == null || (GetType() != obj.GetType()))
                return false;

            var other = obj as OPPToken;
            if (_value.Length != other._value.Length)
                return false;

            for (int i = 0; i < _value.Length; i++)
                if (_value[i] != other._value[i])
                    return false;

            return true;
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        private class OPPTokenFactory : TokenFactory
        {
            public override IToken Parse(string tokenStr)
            {
                return new OPPToken(Encoding.UTF8.GetBytes(tokenStr));
            }

            public override IToken Hash(byte[] partitionKey)
            {
                return new OPPToken(partitionKey);
            }
        }
    }
}