using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class LocalDateTests
    {
        private static readonly Tuple<LocalDate, uint>[] Values = 
        {
            Tuple.Create(new LocalDate(1970, 1, 1),          2147483648U),
            Tuple.Create(new LocalDate(2010, 8, 5),          2147498474U),
            Tuple.Create(new LocalDate(1991, 7, 11),         0x80001eb5),
            Tuple.Create(new LocalDate(1972, 8, 5),          0x800003b3),
            Tuple.Create(new LocalDate(1969, 12, 31),        2147483647U),
            Tuple.Create(new LocalDate(1941, 5, 3),          2147473178U),
            Tuple.Create(new LocalDate(1, 1, 1),             2146764486U),
            Tuple.Create(new LocalDate(0, 1, 1),             2146764120U),
            Tuple.Create(new LocalDate(0, 5, 1),             2146764241U),
            Tuple.Create(new LocalDate(-1, 1, 1),            2146763755U),
            Tuple.Create(new LocalDate(-1, 12, 31),          0x7ff50557U),
            Tuple.Create(new LocalDate(-3, 1, 1),            2146763025U),
            Tuple.Create(new LocalDate(-3, 5, 1),            0x7ff50189U),
            Tuple.Create(new LocalDate(-4, 1, 1),            2146762659U),
            Tuple.Create(new LocalDate(-5, 1, 1),            2146762294U),
            Tuple.Create(new LocalDate(-6, 1, 1),            2146761929U),
            Tuple.Create(new LocalDate(-7, 5, 1),            2146761684U),
            Tuple.Create(new LocalDate(-8, 5, 1),            2146761319U),
            Tuple.Create(new LocalDate(-2, 12, 31),          2146763754U),
            Tuple.Create(new LocalDate(-100, 12, 31),        0x7ff47818U),
            Tuple.Create(new LocalDate(-100, 1, 1),          0x7ff476acU),
            Tuple.Create(new LocalDate(-99, 5, 15),          0x7ff4789fU),
            Tuple.Create(new LocalDate(-99, 1, 1),           0x7ff47819U),
            Tuple.Create(new LocalDate(-5877641, 6, 23),     0U),
            Tuple.Create(new LocalDate(012345, 8, 5),        2151273255),
            Tuple.Create(new LocalDate(062345, 8, 5),        2169535380),
            Tuple.Create(new LocalDate(093456, 8, 5),        0x81fdde88),
            Tuple.Create(new LocalDate(123456, 8, 5),        2191855715),
            Tuple.Create(new LocalDate(5881580, 7, 10),      4294967294U),
            Tuple.Create(new LocalDate(5881580, 7, 11),      uint.MaxValue),
            Tuple.Create(new LocalDate(2399, 12, 31),        2147640701),
            Tuple.Create(new LocalDate(2100, 10, 10),        2147531412),
            Tuple.Create(new LocalDate(2300, 12, 31),        2147604542),
            Tuple.Create(new LocalDate(2400, 12, 31),        2147641067)
        };

        [Test]
        public void Should_Calculate_Days_Centered()
        {
            foreach (var v in Values)
            {
                Assert.AreEqual(v.Item2, v.Item1.DaysSinceEpochCentered, "For Date " + v.Item1);
            }
        }

        [Test]
        public void Should_Calculate_Year_Month_Day_Positives_Dates()
        {
            foreach (var v in Values)
            {
                if (v.Item1.Year < 0)
                {
                    continue;
                }
                var calculated = new LocalDate(v.Item2);
                Assert.AreEqual(v.Item1.Year, calculated.Year, "Year for Date " + v.Item1);
                Assert.AreEqual(v.Item1.Month, calculated.Month, "Month for Date " + v.Item1);
                Assert.AreEqual(v.Item1.Day, calculated.Day, "Day for Date " + v.Item1);
            }
        }

        [Test]
        public void Should_Calculate_Year_Month_Day_Negative_Dates()
        {
            foreach (var v in Values)
            {
                if (v.Item1.Year >= 0)
                {
                    continue;
                }
                var calculated = new LocalDate(v.Item2);
                Assert.AreEqual(v.Item1.Year, calculated.Year, "Year for Date " + v.Item1);
                Assert.AreEqual(v.Item1.Month, calculated.Month, "Month for Date " + v.Item1);
                Assert.AreEqual(v.Item1.Day, calculated.Day, "Day for Date " + v.Item1);
            }
        }

        [Test]
        public void Can_Be_Used_As_Dictionary_Key()
        {
            var dictionary = Values.ToDictionary(v => v.Item1, v => v.Item1.ToString());
            Assert.AreEqual(Values.Length, dictionary.Count);
        }

        [Test]
        public void Should_Support_Operators()
        {
            LocalDate value1 = null;
            Assert.True(value1 == null);
            Assert.False(value1 != null);
            value1 = new LocalDate(2010, 3, 15);
            Assert.False(value1 == null);
            Assert.True(value1 != null);
            Assert.AreEqual(value1, new LocalDate(2010, 3, 15));
        }

        [Test]
        public void ToString_Should_Return_String_Representation()
        {
            var values = new []
            {
                Tuple.Create(2010, 4, 29, "2010-04-29"),
                Tuple.Create(2005, 8, 5,  "2005-08-05"),
                Tuple.Create(101, 10, 5,  "0101-10-05"),
                Tuple.Create(-10, 10, 5,  "-10-10-05"),
                Tuple.Create(-110, 1, 23, "-110-01-23")
            };
            foreach (var v in values)
            {
                Assert.AreEqual(v.Item4, new LocalDate(v.Item1, v.Item2, v.Item3).ToString());
            }
        }

        [Test]
        public void Constructor_Should_Validate_Boundaries()
        {
            var values = new[]
            {
                Tuple.Create(5881581, 1, 1),
                Tuple.Create(5881580, 7, 12),
                Tuple.Create(-5877641, 6, 22),
                Tuple.Create(-5877642, 1, 1)
            };
            foreach (var v in values)
            {
                // ReSharper disable once ObjectCreationAsStatement
                Assert.Throws<ArgumentOutOfRangeException>(() => new LocalDate(v.Item1, v.Item2, v.Item3));
            }
        }

        [Test]
        public void ToDateTimeOffset_Should_Convert()
        {
            var values = new[]
            {
                Tuple.Create(2010, 4, 29),
                Tuple.Create(2005, 8, 5),
                Tuple.Create(101, 10, 5)
            };
            foreach (var v in values)
            {
                var expected = new DateTimeOffset(v.Item1, v.Item2, v.Item3, 0, 0, 0, TimeSpan.Zero);
                Assert.AreEqual(expected, new LocalDate(v.Item1, v.Item2, v.Item3).ToDateTimeOffset());
            }
        }

        [Test]
        public void ToDateTimeOffset_Should_Throw_When_Can_Not_Represent()
        {
            var values = new[]
            {
                Tuple.Create(-1, 4, 29),
                Tuple.Create(0, 8, 5),
                Tuple.Create(10123, 10, 5)
            };
            foreach (var v in values)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new LocalDate(v.Item1, v.Item2, v.Item3).ToDateTimeOffset());
            }
        }

        [Test]
        public void Parse_From_Integer_Text_Values()
        {
            var values = new[]
            {
                Tuple.Create("-1", new LocalDate(1969, 12, 31)),
                Tuple.Create("0", new LocalDate(1970, 1, 1)),
                Tuple.Create("1", new LocalDate(1970, 1, 2))
            };
            foreach (var v in values)
            {
                Assert.AreEqual(v.Item2, LocalDate.Parse(v.Item1));
            }
        }

        [Test]
        public void Parse_From_Standard_Format()
        {
            var values = new[]
            {
                Tuple.Create("1960-6-12", new LocalDate(1960, 6, 12)),
                Tuple.Create("1981-09-14", new LocalDate(1981, 9, 14)),
                Tuple.Create("1-1-1", new LocalDate(1, 1, 1))
            };
            foreach (var v in values)
            {
                Assert.AreEqual(v.Item2, LocalDate.Parse(v.Item1));
            }
        }

        [Test]
        public void Parse_With_Wrong_Format_Should_Throw()
        {
            var values = new[]
            {
                "1960-1",
                "-1909-14",
                "-1-1"
            };
            foreach (var v in values)
            {
                Assert.Throws<FormatException>(() => LocalDate.Parse(v));
            }
            Assert.Throws<ArgumentNullException>(() => LocalDate.Parse(null));
        }
    }
}
