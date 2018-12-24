using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Castle.DynamicProxy.Generators.Emitters.SimpleAST;
using NUnit.Framework;
// ReSharper disable ObjectCreationAsStatement

namespace Cassandra.Tests
{
    [TestFixture]
    public class LocalTimeTests
    {
        private static readonly Tuple<int, int, int, int, long, string>[] Values =
        {
            Tuple.Create(0, 0, 0, 0, 0L, "00:00:00"),
            Tuple.Create(0, 0, 1, 1, 1000000001L, "00:00:01.000000001"),
            Tuple.Create(1, 0, 0, 6001, 3600000006001L, "01:00:00.000006001"),
            Tuple.Create(0, 1, 1, 0, 61000000000L, "00:01:01"),
            Tuple.Create(0, 10, 10, 30000, 610000030000L, "00:10:10.00003"),
            Tuple.Create(14, 29, 31, 800000000, 52171800000000L, "14:29:31.8"),
            Tuple.Create(14, 29, 31, 800600000, 52171800600000L, "14:29:31.8006"),
            Tuple.Create(23, 59, 59, 999999999, 86399999999999L, "23:59:59.999999999")
        };

        [Test]
        public void Should_Calculate_Nanos()
        {
            foreach (var v in Values)
            {
                var time = GetLocalTime(v);
                Assert.AreEqual(v.Item5, time.TotalNanoseconds, "For time: " + v.Item6);
            }
        }

        [Test]
        public void Should_Calculate_Hour_Minute_Second()
        {
            foreach (var v in Values)
            {
                var time = new LocalTime(v.Item5);
                Assert.AreEqual(v.Item1, time.Hour, "For time: " + v.Item6);
                Assert.AreEqual(v.Item2, time.Minute, "For time: " + v.Item6);
                Assert.AreEqual(v.Item3, time.Second, "For time: " + v.Item6);
                Assert.AreEqual(v.Item4, time.Nanoseconds, "For time: " + v.Item6);
            }
        }

        [Test]
        public void LocalTime_Can_Be_Used_As_Dictionary_Key()
        {
            // ReSharper disable once SuggestVarOrType_Elsewhere
            Dictionary<LocalTime, string> dictionary = Values.ToDictionary(GetLocalTime, v => v.Item6);
            Assert.AreEqual(Values.Length, dictionary.Count);
        }

        [Test]
        public void LocalTime_Constructors_Should_Validate_Range()
        {
            var values = new[]
            {
                Tuple.Create(-1, 0, 0, 0),
                Tuple.Create(0, -1, 0, 0),
                Tuple.Create(0, 0, -1, 0),
                Tuple.Create(0, 0, 0, -1),
                Tuple.Create(24, 0, 0, 0),
                Tuple.Create(0, 60, 0, 0),
                Tuple.Create(0, 0, 60, 0),
                Tuple.Create(0, 0, 0, 1000000000)
            };
            foreach (var v in values)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new LocalTime(v.Item1, v.Item2, v.Item3, v.Item4));
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalTime(-1L));
            Assert.Throws<ArgumentOutOfRangeException>(() => new LocalTime(86400000000000L));
        }

        [Test]
        public void LocalTime_ToString_Should_Return_String_Representaion()
        {
            foreach (var v in Values)
            {
                var time = GetLocalTime(v);
                Assert.AreEqual(v.Item6, time.ToString(), "For time: " + v.Item6);
            }
        }

        [Test]
        public void Should_Support_Operators()
        {
            LocalTime value1 = null;
            Assert.True(value1 == null);
            Assert.False(value1 != null);
            value1 = new LocalTime(10, 3, 15, 0);
            Assert.False(value1 == null);
            Assert.True(value1 != null);
            Assert.AreEqual(value1, new LocalTime(10, 3, 15, 0));
        }

        [Test]
        public void LocalTime_Parse_Should_Throw_When_Format_Invalid()
        {
            var values = new[]
            {
                "1",
                "1:1:1:1",
                ""
            };
            foreach (var v in values)
            {
                Assert.Throws<FormatException>(() => LocalTime.Parse(v));
            }
            Assert.Throws<ArgumentNullException>(() => LocalTime.Parse(null));
        }

        [Test]
        public void LocalTime_Parse_Should_Return_A_New_Instance_Based_On_String_Representation()
        {
            foreach (var v in Values)
            {
                var time = LocalTime.Parse(v.Item6);
                Assert.AreEqual(new LocalTime(v.Item5), time);
            }
        }
        
        [Test]
        [TestCase("pt-PT")]
        [TestCase("es-ES")]
        [TestCase("it-IT")]
        [TestCase("en-US")]
        public void LocalTime_ToString_Output_As_Input_For_Parse_Should_Return_An_Equal_LocalTime(string culture)
        {
            var currentCulture = Thread.CurrentThread.CurrentCulture;

            try
            {
                Thread.CurrentThread.CurrentCulture = new CultureInfo(culture);
                var localTime = new LocalTime(15, 21, 50, 1);
                var localTimeStr = localTime.ToString();
                var act = LocalTime.Parse(localTimeStr);
                Assert.AreEqual(localTime, act);
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = currentCulture;
            }
        }

        private static LocalTime GetLocalTime(Tuple<int, int, int, int, long, string> v)
        {
            return new LocalTime(v.Item1, v.Item2, v.Item3, v.Item4);
        }
    }
}
