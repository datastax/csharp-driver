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
    /// Represents a value of type "Instant" in DSE Graph. It's basically a wrapper around a <see cref="DateTimeOffset"/> value.
    /// </summary>
    public struct JavaInstant : IEquatable<JavaInstant>, IEquatable<DateTimeOffset>, IEquatable<DateTime>
    {
        public JavaInstant(DateTimeOffset value)
        {
            Value = value;
        }

        public DateTime AsDateTime() => (DateTime) this;

        public DateTimeOffset AsDateTimeOffset() => (DateTimeOffset) this;

        public DateTimeOffset Value { get; }
        
        public static implicit operator JavaInstant(DateTimeOffset obj) => new JavaInstant(obj);

        public static implicit operator DateTimeOffset(JavaInstant obj) => obj.Value;
        
        public static explicit operator DateTime(JavaInstant obj) => obj.Value.DateTime;

        public static bool operator ==(JavaInstant i1, JavaInstant i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(JavaInstant i1, JavaInstant i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(JavaInstant i1, DateTime i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(JavaInstant i1, DateTime i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(DateTime i1, JavaInstant i2)
        {
            return i2.Equals(i1);
        }

        public static bool operator !=(DateTime i1, JavaInstant i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(JavaInstant i1, DateTimeOffset i2)
        {
            return i1.Equals(i2);
        }

        public static bool operator !=(JavaInstant i1, DateTimeOffset i2)
        {
            return !(i1 == i2);
        }
        
        public static bool operator ==(DateTimeOffset i1, JavaInstant i2)
        {
            return i2.Equals(i1);
        }

        public static bool operator !=(DateTimeOffset i1, JavaInstant i2)
        {
            return !(i1 == i2);
        }

        public bool Equals(JavaInstant other)
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
                case JavaInstant i:
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