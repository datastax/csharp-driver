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
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Cassandra.Serialization.Search;

namespace Cassandra.DataStax.Search
{
    /// <summary>
    /// Represents a range of dates, corresponding to the Apache Solr type
    /// <a href="https://cwiki.apache.org/confluence/display/solr/Working+with+Dates"><c>DateRangeField</c></a>.
    /// <para>
    ///   A date range can have one or two bounds, namely lower bound and upper bound, to represent an interval of time.
    ///   Date range bounds are both inclusive. For example:
    /// </para>
    /// <ul>
    ///   <li><c>[2015 TO 2016-10]</c> represents from the first day of 2015 to the last day of October 2016</li>
    ///   <li><c>2015</c> represents during the course of the year 2015.</li>
    ///   <li><c>[2017 TO *]</c> represents any date greater or equals to the first day of the year 2017.</li>
    /// </ul>
    /// <para>
    /// Note that this representation of <c>DateRangeField</c> does not support Dates outside of the range
    /// supported by <c>DateTimeOffset</c>: from <c>1/1/0001 12:00:00 AM +00:00</c> to <c>12/31/9999 11:59:59 PM +00:00</c>.
    /// </para>
    /// </summary>
    /// <remarks>DateRange instances are immutable and thread-safe.</remarks>
    public struct DateRange : IEquatable<DateRange>, IComparable<DateRange>
    {
        private static readonly Regex MultipleBoundariesRegex = new Regex(
            @"^\[(.+?) TO (.+)]$", RegexOptions.Compiled);

        private static readonly DateRangeSerializer Serializer = new DateRangeSerializer();

        /// <summary>
        /// Gets the lower bound of this range (inclusive).
        /// </summary>
        public DateRangeBound LowerBound { get; private set; }

        /// <summary>
        /// Gets the upper bound of this range (inclusive).
        /// </summary>
        public DateRangeBound? UpperBound { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="DateRange"/> using a lower bound and an upper bound.
        /// <para>Consider using <see cref="Parse(string)"/> to create instances more easily.</para>
        /// </summary>
        /// <param name="lowerBound">
        /// A value representing the range lower bound, composed by a
        /// <c>DateTimeOffset</c> and a precision. Use <see cref="DateRangeBound.Unbounded"/> for an open lower bound.
        /// </param>
        /// <param name="upperBound">
        /// A value representing the range upper bound, composed by a
        /// <c>DateTimeOffset</c> and a precision. Use <see cref="DateRangeBound.Unbounded"/> for an open higher bound.
        /// </param>
        public DateRange(DateRangeBound lowerBound, DateRangeBound upperBound) : this(lowerBound)
        {
            UpperBound = upperBound;
        }

        /// <summary>
        /// Creates a new instance of <see cref="DateRange"/> using a lower bound and an upper bound.
        /// <para>Consider using <see cref="Parse(string)"/> to create instances more easily.</para>
        /// </summary>
        /// <param name="lowerBound">
        /// A value representing the range lower bound, composed by a
        /// <c>DateTimeOffset</c> and a precision. Use <see cref="DateRangeBound.Unbounded"/> for an open lower bound.
        /// </param>
        public DateRange(DateRangeBound lowerBound) : this()
        {
            LowerBound = lowerBound;
        }

        /// <summary>
        /// Returns the <see cref="DateRange"/> representation from a given string.
        /// <para>String representations of dates are always expressed in Coordinated Universal Time(UTC)</para>
        /// </summary>
        /// <exception cref="FormatException" />
        public static DateRange Parse(string value)
        {
            var match = DateRange.MultipleBoundariesRegex.Match(value);
            if (!match.Success)
            {
                return new DateRange(DateRangeBound.Parse(value));
            }
            return new DateRange(
                DateRangeBound.Parse(match.Groups[1].Value), 
                DateRangeBound.ParseUpperBound(match.Groups[2].Value));
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 31 + LowerBound.GetHashCode();
                if (UpperBound != null)
                {
                    hash = hash * 31 + UpperBound.GetHashCode();
                }
                return hash;
            }
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (!(obj is DateRange))
            {
                return false;
            }
            return Equals((DateRange) obj);
        }

        /// <summary>
        /// Returns true if the value of this DateRange instance and other are the same.
        /// </summary>
        public bool Equals(DateRange other)
        {
            return LowerBound.Equals(other.LowerBound) &&
                   (UpperBound != null ? UpperBound.Equals(other.UpperBound) : (other.UpperBound == null));
        }

        /// <summary>
        /// Compares the DateRange based to the bytes representation.
        /// </summary>
        public int CompareTo(DateRange other)
        {
            var buffer1 = DateRange.Serializer.Serialize((ushort)ProtocolVersion.MaxSupported, this);
            var buffer2 = DateRange.Serializer.Serialize((ushort)ProtocolVersion.MaxSupported, other);
            if (buffer1.Length == buffer2.Length)
            {
                // Use Array IStructuralComparable implementation
                return ((IStructuralComparable)buffer1).CompareTo(buffer2, Comparer<byte>.Default);
            }
            var length = Math.Max(buffer1.Length, buffer2.Length);
            // Different length comparison should return as soon as we find a difference starting from msb
            for (var i = 0; i < length; i++)
            {
                byte b1 = 0;
                byte b2 = 0;
                if (buffer1.Length > i)
                {
                    b1 = buffer1[i];
                }
                if (buffer2.Length > i)
                {
                    b2 = buffer2[i];
                }
                if (b1 != b2)
                {
                    return b1 - b2;
                }
            }
            return 0;
        }

        /// <summary>
        /// Returns the string representation of the instance.
        /// </summary>
        public override string ToString()
        {
            if (UpperBound == null)
            {
                return LowerBound.ToString();
            }
            return "[" + LowerBound + " TO " + UpperBound + "]";
        }

        /// <summary>
        /// Compares value equality of 2 DateRange instances.
        /// </summary>
        public static bool operator ==(DateRange a, DateRange b)
        {
            return a.Equals(b);
        }

        /// <summary>
        /// Compares value inequality of 2 DateRange instances.
        /// </summary>
        public static bool operator !=(DateRange a, DateRange b)
        {
            return !(a == b);
        }
    }
}
