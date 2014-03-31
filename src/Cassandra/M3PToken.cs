namespace Cassandra
{
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
}