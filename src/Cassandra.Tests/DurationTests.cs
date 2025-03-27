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
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.Tests
{
    public class DurationTests : BaseUnitTest
    {
        private const long NanosPerMicro = 1000L;
        private const long NanosPerMilli = 1000L * NanosPerMicro;
        private const long NanosPerSecond = 1000L * NanosPerMilli;
        private const long NanosPerMinute = 60L * NanosPerSecond;
        private const long NanosPerHour = 60L * NanosPerMinute;
        private static readonly Tuple<Duration, string, string>[] Values =
        {
            // Duration representation, hex value, standard format
            Tuple.Create(new Duration(0, 0, 1L), "000002", "1ns"),
            Tuple.Create(new Duration(0, 0, 128), "00008100", "128ns"),
            Tuple.Create(new Duration(0, 0, 200), "00008190", "200ns"),
            Tuple.Create(new Duration(0, 0, 2), "000004", "2ns"),
            Tuple.Create(new Duration(0, 0, 256), "00008200", "256ns"),
            Tuple.Create(new Duration(0, 0, 33001), "0000c101d2", "33us1ns"),
            Tuple.Create(new Duration(0, 0, -1), "000001", "-1ns"),
            Tuple.Create(new Duration(0, 0, -33001), "0000c101d1", "-33µs1ns"),
            Tuple.Create(new Duration(0, 0, 0L), "000000", "0"),
            Tuple.Create(new Duration(0, 0, 2251799813685279), "0000fe1000000000003e", "625h29m59s813ms685us279ns"),
            Tuple.Create(new Duration(-1, -1, -1), "010101", "-1mo1d1ns"),
            Tuple.Create(new Duration(1, 1, 1), "020202", "1mo1d1ns"),
            Tuple.Create(new Duration(2, 15, 0), "041e00", "2mo15d"),
            Tuple.Create(new Duration(0, 14, 0), "001c00", "14d"),
            Tuple.Create(new Duration(257, 0, 0), "82020000", "21y5mo"),
            Tuple.Create(new Duration(0, 2, 120000000000), "0004f837e11d6000", "2d2m")
        };

        [Test]
        public void Parse_Should_Read_Standard_Format_Test()
        {
            foreach (var value in Values)
            {
                Assert.AreEqual(Duration.Parse(value.Item3), value.Item1);
            }
        }

        [TestCase(2, 15, 0, "P2M15D")]
        [TestCase(0, 14, 0, "P14D")]
        [TestCase(257, 0, 0, "P21Y5M")]
        [TestCase(0, 2, 120000000000, "P2DT2M")]
        [TestCase(0, 0, 1105000, "PT0.001105S")]
        [TestCase(12, 2, 0, "P1Y2D")]
        [TestCase(14, 0, 0, "P1Y2M")]
        [TestCase(12, 0, 2 * NanosPerHour, "P1YT2H")]
        [TestCase(-14, 0, 0, "-P1Y2M")]
        [TestCase(0, 0, 30 * NanosPerHour, "PT30H")]
        [TestCase(0, 0, 30 * NanosPerHour + 20 * NanosPerMinute, "PT30H20M")]
        [TestCase(0, 0, 20 * NanosPerMinute, "PT20M")]
        [TestCase(0, 0, 56 * NanosPerSecond, "PT56S")]
        [TestCase(15, 0, 130 * NanosPerMinute, "P1Y3MT2H10M")]
        public void Parse_Should_Parse_And_Output_Iso_Format_Test(int months, int days, long nanoseconds, string valueStr)
        {
            var duration = new Duration(months, days, nanoseconds);
            var durationFromString = Duration.Parse(valueStr);
            Assert.AreEqual(duration, durationFromString);
            Assert.AreEqual(valueStr, durationFromString.ToIsoString());
        }

        [Test]
        public void Parse_Should_Read_Iso_Week_Format_Test()
        {
            Assert.AreEqual(Duration.Parse("P1W"), new Duration(0, 7, 0));
            Assert.AreEqual(Duration.Parse("P2W"), new Duration(0, 14, 0));
        }

        [Test]
        public void ToString_Should_Return_Standard_Format_Test()
        {
            foreach (var value in Values)
            {
                var expected = value.Item3.Replace("µs", "us");
                Assert.AreEqual(value.Item1.ToString(), expected);
            }
        }

        [Test]
        public void Equality_Tests()
        {
            foreach (var value in Values)
            {
                var a = value.Item1;
                var b = new Duration(a.Months, a.Days, a.Nanoseconds);
                EqualsTest(a, b);
                var c = Duration.Parse(value.Item3);
                EqualsTest(a, c);
            }
        }

        private static void EqualsTest(Duration a, Duration b)
        {
            Assert.AreEqual(a, b);
            Assert.True(a == b);
            Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
            Assert.False(a.Equals(null));
        }

        [TestCase("567s", "567000ms")]
        [TestCase("1440m", "24h")]
        [TestCase("PT1440M", "PT24H")]
        [TestCase("120m", "2h")]
        [TestCase("2000ms", "2s")]
        [TestCase("1y2mo", "14mo")]
        public void Equality_Tests_From_Parsed_Values(string valueStr1, string valueStr2)
        {
            var duration1 = Duration.Parse(valueStr1);
            var duration2 = Duration.Parse(valueStr2);
            Assert.AreEqual(duration1, duration2);
            Assert.True(duration1 == duration2);
            Assert.AreEqual(duration1.GetHashCode(), duration2.GetHashCode());
        }

        [Test]
        public void DurationSerializer_Should_Serialize()
        {
            var serializer = new DurationSerializer(true);
            foreach (var value in Values)
            {
                Assert.AreEqual(value.Item2, ToHex(serializer.Serialize(4, value.Item1)));
            }
        }

        [Test]
        public void DurationSerializer_Should_Deserialize()
        {
            var serializer = new DurationSerializer(true);
            foreach (var value in Values)
            {
                var buffer = FromHex(value.Item2);
                Assert.AreEqual(value.Item1, serializer.Deserialize(4, buffer, 0, buffer.Length, null));
            }
        }

        [TestCase("1y2mo")]
        [TestCase("-1y2mo")]
        [TestCase("1Y2MO")]
        [TestCase("1y3mo2h10m")]
        [TestCase("P1Y")]
        [TestCase("P1Y2D")]
        [TestCase("P1Y2M")]
        [TestCase("P1YT2H")]
        [TestCase("-P1Y2M")]
        [TestCase("P1Y3MT2H10M")]
        public void ToJavaDurationString_Exception_When_Has_Months(string valueStr)
        {
            var duration = Duration.Parse(valueStr);
            Assert.Throws<ArgumentOutOfRangeException>(() => duration.ToJavaDurationString());
        }

        [Test]
        public void ToTimeSpan_Should_Throw_For_Month_Not_Equals_To_Zero()
        {
            var values = new[]
            {
                new Duration(1, 0, 0),
                new Duration(-10, 0, 0),
                new Duration(500, 1, 1L)
            };
            foreach (var value in values)
            {
                Assert.Throws<InvalidOperationException>(() => value.ToTimeSpan());
            }
        }

        [Test]
        public void ToTimeSpan_FromTimeSpan_Test()
        {
            var values = Values.Select(t => t.Item1).Where(d => d.Months == 0L && d.Nanoseconds % 100L == 0L);
            foreach (var value in values)
            {
                var timespan = value.ToTimeSpan();
                Assert.AreEqual(value, Duration.FromTimeSpan(timespan));
            }
        }
    }
}
