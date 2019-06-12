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
using System.Linq;
using System.Numerics;
using Cassandra.Serialization;
using Cassandra.Serialization.Primitive;

namespace Cassandra.Tests.Extensions.Serializers
{
    /// <summary>
    /// A BigDecimal serializer
    /// </summary>
    public class BigDecimalSerializer : TypeSerializer<BigDecimal>
    {
        private readonly BigIntegerSerializer _bigIntegerSerializer = new BigIntegerSerializer();

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Decimal; }
        }

        public override BigDecimal Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var scale = BeConverter.ToInt32(buffer, offset);
            var unscaledValue = _bigIntegerSerializer.Deserialize(protocolVersion, buffer, 4, length - 4, null);
            return new BigDecimal(scale, unscaledValue);
        }

        public override byte[] Serialize(ushort protocolVersion, BigDecimal value)
        {
            var scaleBuffer = BeConverter.GetBytes(value.Scale);
            var valueBuffer = _bigIntegerSerializer.Serialize(protocolVersion, value.UnscaledValue);
            return Utils.JoinBuffers(new[] {scaleBuffer, valueBuffer}, scaleBuffer.Length + valueBuffer.Length);
        }
    }

    /// <summary>
    /// A basic BigDecimal representation.
    /// </summary>
    public class BigDecimal
    {
        public int Scale { get; private set; }
        public BigInteger UnscaledValue { get; private set; }

        public BigDecimal(int scale, BigInteger unscaledValue)
        {
            Scale = scale;
            UnscaledValue = unscaledValue;
        }

        public override string ToString()
        {
            var intString = UnscaledValue.ToString();
            if (Scale == 0) 
            {
                return intString;
            }
            var signSymbol = "";
            if (intString[0] == '-')
            {
                signSymbol = "-";
                intString = intString.Substring(1);
            }
            var separatorIndex = intString.Length - Scale;
            if (separatorIndex <= 0)
            {
                //add zeros at the beginning, plus an additional zero
                intString = string.Join("", Enumerable.Repeat("0", (-separatorIndex) + 1)) + intString;
                separatorIndex = intString.Length - Scale;
            }
            return signSymbol + intString.Substring(0, separatorIndex) + '.' + intString.Substring(separatorIndex);
        }

        public override bool Equals(object obj)
        {
            var other = obj as BigDecimal;
            if (other == null)
            {
                return false;
            }
            return Scale == other.Scale && UnscaledValue == other.UnscaledValue;
        }

        public override int GetHashCode()
        {
            var hash = 17 * 23 + Scale.GetHashCode();
            return hash * 23 + UnscaledValue.GetHashCode();
        }
    }
}
