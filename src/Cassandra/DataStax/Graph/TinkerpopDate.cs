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

namespace Cassandra.DataStax.Graph
{
    /// <summary>
    /// Represents a value of type "Date" in Tinkerpop. It's basically a wrapper around a <see cref="DateTimeOffset"/> value.
    /// </summary>
    public struct TinkerpopDate : IEquatable<TinkerpopDate>, IEquatable<DateTimeOffset>, IEquatable<DateTime>
    {
        public TinkerpopDate(DateTimeOffset value)
        {
            Value = value;
        }

        public DateTime AsDateTime() => (DateTime) this;

        public DateTimeOffset AsDateTimeOffset() => (DateTimeOffset) this;

        public DateTimeOffset Value { get; }
        
        public static implicit operator TinkerpopDate(DateTimeOffset obj) => new TinkerpopDate(obj);

        public static implicit operator DateTimeOffset(TinkerpopDate obj) => obj.Value;
        
        public static explicit operator DateTime(TinkerpopDate obj) => obj.Value.DateTime;

        public static bool operator ==(TinkerpopDate i1, TinkerpopDate i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(TinkerpopDate i1, TinkerpopDate i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(TinkerpopDate i1, DateTime i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(TinkerpopDate i1, DateTime i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(DateTime i1, TinkerpopDate i2)
        {
            return i2.Equals(i1);
        }

        public static bool operator !=(DateTime i1, TinkerpopDate i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(TinkerpopDate i1, DateTimeOffset i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(TinkerpopDate i1, DateTimeOffset i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(DateTimeOffset i1, TinkerpopDate i2)
        {
            return i2.Equals(i1);
        }

        public static bool operator !=(DateTimeOffset i1, TinkerpopDate i2)
        {
            return !(i1 == i2);
        }

        public bool Equals(TinkerpopDate other)
        {
            return Value.Equals(other.Value);
        }

        public bool Equals(DateTimeOffset other)
        {
            return AsDateTimeOffset().Equals(other);
        }

        public bool Equals(DateTime other)
        {
            return AsDateTime().Equals(other);
        }

        public override int GetHashCode()
        {
            return -1937169414 + Value.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case null:
                    return false;
                case DateTime dt:
                    return Equals(dt);
                case DateTimeOffset dto:
                    return Equals(dto);
                case TinkerpopDate i:
                    return Equals(i);
                default:
                    return false;
            }
        }

        public override string ToString()
        {
            return Value.ToString();
        }
        
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return Value.ToString(format, formatProvider);
        }

        public string ToString(string format)
        {
            return Value.ToString(format);
        }

        public string ToString(IFormatProvider formatProvider)
        {
            return Value.ToString(formatProvider);
        }
    }
}