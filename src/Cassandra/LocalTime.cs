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
using System.Globalization;

namespace Cassandra
{
    /// <summary>
    /// A time without a time-zone in the ISO-8601 calendar system, such as 10:30:05.
    /// LocalTime is an immutable date-time object that represents a time, often viewed as hour-minute-second. 
    /// Time is represented to nanosecond precision. For example, the value "13:45.30.123456789" can be stored in a LocalTime.
    /// </summary>
    public class LocalTime : IComparable<LocalTime>, IEquatable<LocalTime>
    {
        private const long MaxNanos = 86399999999999L;
        private const long NanosInMilliseconds = 1000000L;
        private const long NanosInSeconds = 1000L * NanosInMilliseconds;
        private const long NanosInMinutes = 60L * NanosInSeconds;
        private const long NanosInHour = 60L * NanosInMinutes;

        /// <summary>
        /// Gets the number of nanoseconds since midnight.
        /// </summary>
        public long TotalNanoseconds { get; private set; }

        /// <summary>
        /// Gets the hour component of the time represented by the current instance, a number from 0 to 23.
        /// </summary>
        public int Hour
        {
            get { return (int)(TotalNanoseconds / NanosInHour); }
        }

        /// <summary>
        /// Gets the minute component of the time represented by the current instance, a number from 0 to 59.
        /// </summary>
        public int Minute
        {
            get { return (int)(TotalNanoseconds / NanosInMinutes) % 60; }
        }

        /// <summary>
        /// Gets the second component of the time represented by the current instance, a number from 0 to 59.
        /// </summary>
        public int Second
        {
            get { return (int)(TotalNanoseconds / NanosInSeconds) % 60; }
        }

        /// <summary>
        /// Gets the nanoseconds component of the time represented by the current instance, a number from 0 to 999,999,999.
        /// </summary>
        public int Nanoseconds
        {
            get { return (int)(TotalNanoseconds % NanosInSeconds) % 1000000000; }
        }

        /// <summary>
        /// Creates a new instance based on nanoseconds since midnight.
        /// </summary>
        /// <param name="totalNanoseconds">Nanoseconds since midnight. Valid values are in the range 0 to 86399999999999.</param>
        public LocalTime(long totalNanoseconds)
        {
            if (totalNanoseconds < 0L || totalNanoseconds > MaxNanos)
            {
                throw new ArgumentOutOfRangeException("totalNanoseconds", "Valid total nanoseconds values are in the range 0 to 86399999999999");
            }
            TotalNanoseconds = totalNanoseconds;
        }

        /// <summary>
        /// Creates a new instance based on the specified hour, minutes, seconds, millis and nanoseconds.
        /// </summary>
        /// <param name="hour">A number between 0 and 23 representing the hour portion of the time</param>
        /// <param name="minute">A number between 0 and 59 representing the minutes portion of the time</param>
        /// <param name="second">A number between 0 and 59 representing the seconds portion of the time</param>
        /// <param name="nanosecond">A number between 0 and 999,999,999  representing the seconds portion of the time</param>
        public LocalTime(int hour, int minute, int second, int nanosecond)
        {
            if (hour < 0 || hour > 23)
            {
                throw new ArgumentOutOfRangeException("hour", "Hour must be a number between 0 and 23");
            }
            if (minute < 0 || minute > 59)
            {
                throw new ArgumentOutOfRangeException("minute", "Minute must be a number between 0 and 59");
            }
            if (second < 0 || second > 59)
            {
                throw new ArgumentOutOfRangeException("second", "Second must be a number between 0 and 59");
            }
            if (nanosecond < 0 || nanosecond > 999999999)
            {
                throw new ArgumentOutOfRangeException("nanosecond", "Nanosecond must be a number between 0 and 999,999,999");
            }
            TotalNanoseconds =
                hour*NanosInHour +
                minute*NanosInMinutes +
                second*NanosInSeconds +
                nanosecond;
        }

        /// <summary>
        /// Creates a new <see cref="LocalTime"/> instance based on the string representation.
        /// </summary>
        public static LocalTime Parse(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            var parts = value.Split(':');
            if (parts.Length < 2 || parts.Length > 3)
            {
                throw new FormatException("LocalTime format is invalid");
            }
            var seconds = 0;
            var nanos = 0;
            if (parts.Length == 3)
            {
                var decimalSeconds = Convert.ToDecimal(parts[2], CultureInfo.InvariantCulture);
                var integralPart = Math.Truncate(decimalSeconds);
                seconds = (int) integralPart;
                nanos = (int)(NanosInSeconds * (decimalSeconds - integralPart));
            }
            return new LocalTime(Convert.ToInt32(parts[0]), Convert.ToInt32(parts[1]), seconds, nanos);
        }

        public override string ToString()
        {
            var nanosPart = "";
            var nanos = Nanoseconds;
            if (nanos > 0)
            {
                nanosPart = Utils.FillZeros(nanos, 9);
                
                var lastPosition = 0;
                for (var i = nanosPart.Length - 1; i > 0; i--) 
                {
                    if (nanosPart[i] != '0') 
                    {
                        break;
                    }
                    lastPosition = i;
                }
                if (lastPosition > 0) 
                {
                    nanosPart = nanosPart.Substring(0, lastPosition);
                }
                nanosPart = "." + nanosPart;
            }
            return
                Utils.FillZeros(Hour) + ":" +
                Utils.FillZeros(Minute) + ":" +
                Utils.FillZeros(Second) +
                nanosPart;
        }

        public int CompareTo(LocalTime other)
        {
            if ((object)other == null)
            {
                return 1;
            }
            return TotalNanoseconds.CompareTo(other.TotalNanoseconds);
        }

        public bool Equals(LocalTime other)
        {
            return CompareTo(other) == 0;
        }

        public override int GetHashCode()
        {
            return TotalNanoseconds.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var other = obj as LocalTime;
            return other != null && CompareTo(other) == 0;
        }

        public static bool operator ==(LocalTime value1, LocalTime value2)
        {
            // If both are null, or both are same instance, return true.
            if (ReferenceEquals(value1, value2))
            {
                return true;
            }
            // If one is null, but not both, return false.
            if (((object)value1 == null) || ((object)value2 == null))
            {
                return false;
            }
            return value1.Equals(value2);
        }

        public static bool operator >=(LocalTime value1, LocalTime value2)
        {
            return value1.CompareTo(value2) >= 0;
        }

        public static bool operator >(LocalTime value1, LocalTime value2)
        {
            return value1.CompareTo(value2) > 0;
        }

        public static bool operator <=(LocalTime value1, LocalTime value2)
        {
            return value1.CompareTo(value2) <= 0;
        }

        public static bool operator <(LocalTime value1, LocalTime value2)
        {
            return value1.CompareTo(value2) < 0;
        }

        public static bool operator !=(LocalTime value1, LocalTime value2)
        {
            // If both are null, or both are same instance, return false.
            if (ReferenceEquals(value1, value2))
            {
                return false;
            }
            // If one is null, but not both, return true.
            if (((object)value1 == null) || ((object)value2 == null))
            {
                return true;
            }
            return !value1.Equals(value2);
        }
    }
}
