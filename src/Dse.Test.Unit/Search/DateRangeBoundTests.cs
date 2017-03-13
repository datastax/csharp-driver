using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dse.Search;
using NUnit.Framework;

namespace Dse.Test.Unit.Search
{
    public class DateRangeBoundTests : BaseUnitTest
    {
        [Test]
        public void Parse_ToString_Test()
        {
            var arr = new[]
            {
                "2017-01-20T06:24:57.123Z",
                "2017-01-20T06",
                "2017",
                "1234-07",
                "1234-07-02T18:01",
                "0001",
                "*"
            };
            foreach (var boundaryString in arr)
            {
                var value = DateRangeBound.Parse(boundaryString);
                Assert.AreEqual(boundaryString, value.ToString());
            }
        }

        [Test]
        public void Constructor_Validates_Precision()
        {
            var values = new byte[] { 8, 9, 32, 128, 255 };
            foreach (var invalidPrecision in values)
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => new DateRangeBound(DateTimeOffset.UtcNow, (DateRangePrecision)invalidPrecision));
            }
        }

        [Test]
        public void Constructor_Uses_Utc_Timestamp()
        {
            var d1 = new DateTimeOffset(1999, 12, 31, 23, 59, 59, TimeSpan.FromHours(3));
            Assert.AreEqual(new DateRangeBound(d1, DateRangePrecision.Millisecond).ToString(), "1999-12-31T20:59:59.000Z");
        }

        [Test]
        public void Equality_Tests()
        {
            var d1 = new DateTimeOffset(1999, 12, 31, 23, 59, 59, TimeSpan.FromHours(0));
            var boundary1 = new DateRangeBound(d1, DateRangePrecision.Millisecond);
            Assert.True(boundary1 == new DateRangeBound(d1, DateRangePrecision.Millisecond));
            Assert.True(boundary1.Equals(new DateRangeBound(d1, DateRangePrecision.Millisecond)));
            Assert.False(boundary1 == new DateRangeBound(d1, DateRangePrecision.Second));
            Assert.True(boundary1 != new DateRangeBound(d1, DateRangePrecision.Second));
            Assert.False(boundary1.Equals(new DateRangeBound(d1, DateRangePrecision.Second)));
            Assert.False(boundary1.Equals(null));
        }
    }
}
