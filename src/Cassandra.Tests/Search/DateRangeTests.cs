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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Search;
using NUnit.Framework;
using Precision = Cassandra.Search.DateRangePrecision;

namespace Cassandra.Tests.Search
{
    public class DateRangeTests : BaseUnitTest
    {
        internal static readonly Tuple<string, string, DateRange>[] Values = {
            Tuple.Create("[2010 TO 2011-12]", (string)null, 
                GetRange(UtcDate(2010), Precision.Year, UtcDate(2011, 12, 31, 23, 59, 59, 999), Precision.Month)),
            Tuple.Create("[* TO 2011-8]", "[* TO 2011-08]", 
                GetRange(null, null, UtcDate(2011, 8, 31, 23, 59, 59, 999), Precision.Month)),
            Tuple.Create("[2015-01 TO *]", (string)null, 
                new DateRange(new DateRangeBound(UtcDate(2015), Precision.Month), DateRangeBound.Unbounded)),
            Tuple.Create("[2017-01 TO 2017-02]", "[2017-01 TO 2017-02]", 
                GetRange(UtcDate(2017), Precision.Month, UtcDate(2017, 2, 28, 23, 59, 59, 999), Precision.Month)),
            Tuple.Create("[2016-1 TO 2016-02]", "[2016-01 TO 2016-02]", 
                GetRange(UtcDate(2016), Precision.Month, UtcDate(2016, 2, 29, 23, 59, 59, 999), Precision.Month)),
            Tuple.Create("2012-1-2", "2012-01-02", GetRange(UtcDate(2012, 1, 2), Precision.Day)),
            Tuple.Create("2012-1-2T", "2012-01-02", GetRange(UtcDate(2012, 1, 2), Precision.Day)),
            Tuple.Create("1-2-3T23:5:7", "0001-02-03T23:05:07", 
                GetRange(UtcDate(1, 2, 3, 23, 5, 7), Precision.Second)),
            Tuple.Create("2015-01T03", "2015-01-01T03", GetRange(UtcDate(2015, 1, 1, 3), Precision.Hour)),
            Tuple.Create("2015-04T03:02", "2015-04-01T03:02", 
                GetRange(UtcDate(2015, 4, 1, 3, 2), Precision.Minute)),
            Tuple.Create("2015-04T03:02:01.081", "2015-04-01T03:02:01.081Z", 
                GetRange(UtcDate(2015, 4, 1, 3, 2, 1, 81), Precision.Millisecond)),
            Tuple.Create("*", (string)null, new DateRange(DateRangeBound.Unbounded)),
            Tuple.Create("[* TO *]", (string)null, new DateRange(DateRangeBound.Unbounded, DateRangeBound.Unbounded)),
            Tuple.Create("0001-01-01", (string)null, new DateRange(new DateRangeBound(UtcDate(1), Precision.Day))),

        };


        [Test]
        public void Parse_Values_Test()
        {
            foreach (var value in Values)
            {
                Assert.AreEqual(DateRange.Parse(value.Item1), value.Item3);
            }
        }

        [Test]
        public void Parse_Should_Throw_For_Invalid_Values()
        {
            var invalidValues = new[]
            {
                "2015-01T03:02.001",
                "2012-1-2T12:",
                "2015-01T03.001",
                "[2015-01 TO]",
                "[ TO 2015-01]",
                "[TO 2015-01]",
                " 2015-01",
                "2015-01T03:04.001"
            };
            foreach (var value in invalidValues)
            {
                Assert.Throws<FormatException>(() => DateRange.Parse(value));
            }
        }

        [Test]
        public void ToString_Values_Test()
        {
            foreach (var value in Values)
            {
                Assert.AreEqual(value.Item3.ToString(), value.Item2 ?? value.Item1);
            }
        }

        [Test]
        public void Constructor_Sets_UpperBound_To_Null_Test()
        {
            var lowerBound = new DateRangeBound(DateTimeOffset.UtcNow, DateRangePrecision.Second);
            var value = new DateRange(lowerBound);
            Assert.AreEqual(null, value.UpperBound);
            Assert.AreEqual(lowerBound, value.LowerBound);
        }

        [Test]
        public void Compare_Should_Be_Based_By_Byte_Order()
        {
            var valuesToCompare = new[]
            {
                Tuple.Create("2001-01-01", 10),
                Tuple.Create("2002-01-01", 20),
                Tuple.Create("2002-01-02", 21),
                Tuple.Create("[2000-01-01 TO 2000-01-01]", 30),
                Tuple.Create("[* TO 2000-01-01]", 40),
                Tuple.Create("[* TO 2001-01-01]", 41),
                Tuple.Create("[* TO 2002-01-01]", 42),
                Tuple.Create("[* TO *]", 50)
            };
            foreach (var value in valuesToCompare)
            {
                foreach (var other in valuesToCompare)
                {
                    Assert.AreEqual(
                        Math.Sign(value.Item2.CompareTo(other.Item2)), 
                        Math.Sign(DateRange.Parse(value.Item1).CompareTo(DateRange.Parse(other.Item1))),
                        "Comparison failed for {0} vs {1}", value.Item1, other.Item1);
                }
            }
        }

        private static DateTimeOffset UtcDate(
            int year, int month = 1, int day = 1, int hour = 0, int minute = 0, int second = 0, int ms = 0)
        {
            return new DateTimeOffset(year, month, day, hour, minute, second, ms, TimeSpan.Zero);
        }

        private static DateRange GetRange(
            DateTimeOffset? d1, Precision? precision1, DateTimeOffset? d2 = null, Precision? precision2 = null)
        {
            var lowerBound = DateRangeBound.Unbounded;
            if (d1 != null && precision1 != null)
            {
                lowerBound = new DateRangeBound(d1.Value, precision1.Value);
            }
            if (d2 == null || precision2 == null)
            {
                return new DateRange(lowerBound);
            }
            return new DateRange(lowerBound, new DateRangeBound(d2.Value, precision2.Value));
        }
    }
}
