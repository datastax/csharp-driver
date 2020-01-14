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
using System.Text.RegularExpressions;

namespace Cassandra.DataStax.Search
{
    /// <summary>
    /// Represents a date range boundary, composed by a <c>Date</c> and a precision.
    /// </summary>
    /// <remarks>DateRangeBound instances are immutable and thread-safe.</remarks>
    public struct DateRangeBound : IEquatable<DateRangeBound>
    {
        /// <summary>
        /// Regex to parse dates in the following format YYYY-MM-DDThh:mm:ss.mssZ.
        /// </summary>
        private static readonly Regex TimestampRegex = new Regex(
            // year mandatory 1 to 4 digits
            @"^[-+]?(\d{1,4})" +
            // two non-capturing groups representing the month and day (1 to 2 digits captured)
            @"(?:-(\d{1,2}))?(?:-(\d{1,2}))?" +
            // A non-capturing group for the time portion
            @"(?:T(\d{1,2}?)?(?::(\d{1,2}))?(?::(\d{1,2})(?:\.(\d{1,3}))?)?)?Z?$", RegexOptions.Compiled);

        private static readonly string[] FormatByPrecision =
        {
            "{0:0000}",
            "{0:0000}-{1:00}",
            "{0:0000}-{1:00}-{2:00}",
            "{0:0000}-{1:00}-{2:00}T{3:00}",
            "{0:0000}-{1:00}-{2:00}T{3:00}:{4:00}",
            "{0:0000}-{1:00}-{2:00}T{3:00}:{4:00}:{5:00}",
            "{0:0000}-{1:00}-{2:00}T{3:00}:{4:00}:{5:00}.{6:000}Z"
        };

        /// <summary>
        /// The unbounded <see cref="DateRangeBound"/> instance. Unbounded bounds are syntactically
        /// represented by a <c>*</c> (star) sign.
        /// </summary>
        public static readonly DateRangeBound Unbounded = new DateRangeBound(true);

        private const string UnboundedString = "*";

        private readonly DateTimeOffset _timestamp;
        private readonly DateRangePrecision _precision;

        /// <summary>
        /// The timestamp portion of the boundary.
        /// </summary>
        public DateTimeOffset Timestamp
        {
            get { return _timestamp; }
        }

        /// <summary>
        /// The precision portion of the boundary.
        /// </summary>
        public DateRangePrecision Precision
        {
            get { return _precision; }
        }

        /// <summary>
        /// Creates a new instance of <see cref="DateRangeBound"/>.
        /// </summary>
        public DateRangeBound(DateTimeOffset timestamp, DateRangePrecision precision) : 
            this(precision, timestamp.ToUniversalTime())
        {
        }

        /// <summary>
        /// Creates a new instance of <see cref="DateRangeBound"/> using a UTC timestamp
        /// </summary>
        /// <param name="precision"></param>
        /// <param name="utcTimestamp"></param>
        private DateRangeBound(DateRangePrecision precision, DateTimeOffset utcTimestamp)
        {
            if (precision < DateRangePrecision.Year || precision > DateRangePrecision.Millisecond)
            {
                throw new ArgumentOutOfRangeException("precision");
            }
            if (utcTimestamp.Offset != TimeSpan.Zero)
            {
                throw new ArgumentException("Timestamp should be a UTC time", "utcTimestamp");
            }
            _timestamp = utcTimestamp;
            _precision = precision;
        }
        
        /// <summary>
        /// Private constructor only intended for creating the unbounded instance
        /// </summary>
        // ReSharper disable once UnusedParameter.Local
        private DateRangeBound(bool asUnbounded)
        {
            // Workaround: we can not declare a parameter-less constructor on a struct
            _timestamp = DateTimeOffset.MinValue;
            _precision = (DateRangePrecision) byte.MaxValue;
        }

        /// <summary>
        /// Returns true if the value of this instance and other are the same.
        /// </summary>
        public bool Equals(DateRangeBound other)
        {
            return other.Precision == Precision && other.Timestamp == Timestamp;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (!(obj is DateRangeBound))
            {
                return false;
            }
            return Equals((DateRangeBound) obj);
        }
        
        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 31 + Timestamp.GetHashCode();
                hash = hash * 31 + Precision.GetHashCode();
                return hash;
            }
        }

        /// <summary>
        /// Returns the string representation of the instance.
        /// </summary>
        public override string ToString()
        {
            if ((byte)_precision == byte.MaxValue)
            {
                return "*";
            }
            return string.Format(DateRangeBound.FormatByPrecision[(int) Precision],
                Timestamp.Year, Timestamp.Month, Timestamp.Day, 
                Timestamp.Hour, Timestamp.Minute, Timestamp.Second, Timestamp.Millisecond);

        }

        /// <summary>
        /// Returns the <see cref="DateRangeBound"/> representation of a given string.
        /// <para>String representations of dates are always expressed in Coordinated Universal Time(UTC)</para>
        /// </summary>
        /// <exception cref="FormatException" />
        public static DateRangeBound Parse(string boundaryString)
        {
            if (boundaryString == DateRangeBound.UnboundedString)
            {
                return DateRangeBound.Unbounded;
            }
            return DateRangeBound.ParseBuilder(boundaryString).Build();
        }

        internal static DateRangeBound ParseUpperBound(string boundaryString)
        {
            if (boundaryString == DateRangeBound.UnboundedString)
            {
                return DateRangeBound.Unbounded;
            }
            var builder = DateRangeBound.ParseBuilder(boundaryString);
            return builder.BuildUpperBound();
        }

        private static Builder ParseBuilder(string boundaryString)
        {
            var match = DateRangeBound.TimestampRegex.Match(boundaryString);
            if (!match.Success)
            {
                throw new FormatException("String provided is not a valid timestamp " + boundaryString);
            }
            if (match.Groups[7].Success && !match.Groups[5].Success)
            {
                // Due to a limitation in the regex, its possible to match dates like 2015T03:02.001, 
                // without the seconds portion but with the milliseconds specified.
                throw new FormatException("String representation of the date contains the milliseconds " +
                                          "portion but not the seconds: " + boundaryString);
            }
            if (boundaryString[0] == '-')
            {
                throw new ArgumentOutOfRangeException("boundaryString",
                    "Dates outside the DateTimeOffset range are not supported");
            }
            var builder = new Builder();
            for (var i = 1; i < match.Groups.Count; i++)
            {
                builder.Set(i - 1, match.Groups[i].Value, boundaryString);
            }
            return builder;
        }

        /// <summary>
        /// Compares value equality of 2 DateRangeBound instances.
        /// </summary>
        public static bool operator ==(DateRangeBound a, DateRangeBound b)
        {
            return a.Equals(b);
        }

        /// <summary>
        /// Compares value inequality of 2 DateRangeBound instances.
        /// </summary>
        public static bool operator !=(DateRangeBound a, DateRangeBound b)
        {
            return !(a == b);
        }

        private class Builder
        {
            private int _index;
            private readonly int[] _values = new int[7];

            public void Set(int index, string value, string stringDate)
            {
                if (string.IsNullOrEmpty(value))
                {
                    return;
                }
                if (index > 6)
                {
                    throw new IndexOutOfRangeException();
                }
                if (index > _index)
                {
                    _index = index;
                }
                var numValue = int.Parse(value);
                switch ((DateRangePrecision)index)
                {
                    case DateRangePrecision.Month:
                        if (numValue < 1 || numValue > 12)
                        {
                            throw new ArgumentOutOfRangeException(
                                "Month portion is not valid for date: " + stringDate);
                        }
                        break;
                    case DateRangePrecision.Day:
                        if (numValue < 1 || numValue > 31)
                        {
                            throw new ArgumentOutOfRangeException(
                                "Day portion is not valid for date: " + stringDate);
                        }
                        break;
                    case DateRangePrecision.Hour:
                        if (numValue > 23)
                        {
                            throw new ArgumentOutOfRangeException(
                                "Hour portion is not valid for date: " + stringDate);
                        }
                        break;
                    case DateRangePrecision.Minute:
                    case DateRangePrecision.Second:
                        if (numValue > 59)
                        {
                            throw new ArgumentOutOfRangeException(
                                "Minute/second portion is not valid for date: " + stringDate);
                        }
                        break;
                    case DateRangePrecision.Millisecond:
                        if (numValue > 999)
                        {
                            throw new ArgumentOutOfRangeException(
                                "Millisecond portion is not valid for date: " + stringDate);
                        }
                        break;
                }
                _values[index] = numValue;
            }

            private void Set(DateRangePrecision precision, int value)
            {
                _values[(int) precision] = value;
            }

            public DateRangeBound Build()
            {
                var timestamp = new DateTimeOffset(
                    // year, month, day
                    _values[0], Math.Max(_values[1], 1), Math.Max(_values[2], 1),
                    // hour, minutes, second, millisecond
                    _values[3], _values[4], _values[5], _values[6],
                    TimeSpan.Zero);
                return new DateRangeBound((DateRangePrecision)_index, timestamp);
            }

            public DateRangeBound BuildUpperBound()
            {
                var precision = (DateRangePrecision) _index;
                if (precision == DateRangePrecision.Year)
                {
                    Set(DateRangePrecision.Month, 12);
                }
                if (precision <= DateRangePrecision.Month)
                {
                    Set(DateRangePrecision.Day, DateTime.DaysInMonth(_values[0], Math.Max(_values[1], 1)));
                }
                if (precision <= DateRangePrecision.Day)
                {
                    Set(DateRangePrecision.Hour, 23);
                }
                if (precision <= DateRangePrecision.Hour)
                {
                    Set(DateRangePrecision.Minute, 59);
                }
                if (precision <= DateRangePrecision.Minute)
                {
                    Set(DateRangePrecision.Second, 59);
                }
                if (precision <= DateRangePrecision.Second)
                {
                    Set(DateRangePrecision.Millisecond, 999);
                }
                return Build();
            }
        }
    }
}
