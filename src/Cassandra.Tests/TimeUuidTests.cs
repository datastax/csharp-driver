using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    class TimeUuidTests
    {
        [Test]
        public void GenerationDateTest()
        {
            var dates = new[]
            {
                new DateTimeOffset(1960, 12, 26, 11, 12, 13, TimeSpan.FromHours(-8)),
                new DateTimeOffset(1999, 8, 14, 11, 12, 14, 670, TimeSpan.Zero),
                new DateTimeOffset(1698, 8, 14, 11, 12, 14, 671, TimeSpan.Zero),
                new DateTimeOffset(2020, 5, 2, 1, 3, 30, 455, TimeSpan.Zero),
                new DateTimeOffset(2003, 5, 30, 12, 2, 30, 998, TimeSpan.Zero)
            };
            foreach (var date in dates)
            {
                var id = TimeUuid.NewId(date);
                var dateObtained = id.GetDate();
                Assert.AreEqual(date, dateObtained);
            }
        }

        [Test]
        public void UuidEqualityTest()
        {
            var date = new DateTimeOffset(12021020116L, TimeSpan.Zero);
            var node1 = new byte[] { 0, 1, 2, 3, 4, 5 };
            var clock1 = new byte[] { 0xff, 0x01 };
            var id1 = TimeUuid.NewId(node1, clock1, date);
            var id2 = TimeUuid.NewId(node1, clock1, date);
            var id3 = TimeUuid.NewId(new byte[] { 0, 0, 0, 0, 0, 0 }, new byte[] { 0xff, 0x02 }, date);
            var id4 = TimeUuid.NewId(node1, clock1, new DateTimeOffset());
            Assert.AreEqual(id1, id2);
            Assert.True(id1 == id2);
            Assert.AreNotEqual(id1, id3);
            Assert.True(id1 != id3);
            Assert.AreNotEqual(id1, id4);
            Assert.True(id1 != id4);
            Assert.AreNotEqual(id3, id4);
            Assert.True(id3 != id4);
        }
    }
}
