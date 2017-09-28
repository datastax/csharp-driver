using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TimeUuidTests
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
            Assert.IsTrue(id1.Equals(id2));
            Assert.IsTrue(id1.Equals((object)id2));
            Assert.IsFalse(id1.Equals(string.Empty));
            Assert.IsFalse(id1.Equals(null));
            Assert.AreNotEqual(id1, id3);
            Assert.True(id1 != id3);
            Assert.AreNotEqual(id1, id4);
            Assert.True(id1 != id4);
            Assert.AreNotEqual(id3, id4);
            Assert.True(id3 != id4);
            //Check implicit conversion operators
            var id4Id = (Guid)id4;
            var id4TimeId = (TimeUuid)id4Id;
            Assert.AreEqual(id4, id4TimeId);
            Assert.AreEqual(id4.ToByteArray(), id4Id.ToByteArray());
        }
        
        [Test]
        public void CheckCollisionsTest()
        {
            var values = new Dictionary<string, bool>();
            var date = DateTimeOffset.Now;
            for (var i = 0; i < 1000000; i++)
            {
                //The node id and clock id should be pseudo random
                var id = TimeUuid.NewId(date).ToString();
                Assert.False(values.ContainsKey(id), "TimeUuid collision at position {0}", i);
                values.Add(id, true);
            }
        }

        [Test]
        public void CheckCollisionsParallelTest()
        {
            var values = new ConcurrentDictionary<string, bool>();
            var date = DateTimeOffset.Now;
            var actions = new List<Action>(500000);
            for (var i = 0; i < 500000; i++)
            {
                Action a = () =>
                {
                    //The node id and clock id should be pseudo random
                    var id = TimeUuid.NewId(date).ToString();
                    Assert.False(values.ContainsKey(id), "TimeUuid collided");
                    values.AddOrUpdate(id, true, (k, o) => true);
                };
                actions.Add(a);
            }

            Parallel.Invoke(actions.ToArray());
        }

        [Test]
        public void EnsureIsEquatable()
        {
            Assert.That(new TimeUuid(), Is.InstanceOf<IEquatable<TimeUuid>>());
        }

        [Test]
        public void EnsureIsComparable()
        {
            Assert.That(new TimeUuid(), Is.InstanceOf<IComparable<TimeUuid>>());
        }

        [Test]
        public void Compare_Test1()
        {
            var date = DateTimeOffset.UtcNow;
            var previousId = TimeUuid.NewId(date);
            for (var i = 1; i < 50 * 1000; i += 100)
            {
                var id = TimeUuid.NewId(date.AddTicks(i));
                Assert.AreEqual(1, id.CompareTo(previousId), id.GetDate().Ticks + " greater than " + previousId.GetDate().Ticks + "?");

                previousId = id;
            }
        }

        [Test]
        public void Compare_Test2()
        {
            var dt0 = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);
            var arr = new []
            {
                TimeUuid.NewId(dt0.AddTicks(0x00000000007fffL)),
                TimeUuid.NewId(dt0.AddTicks(0x00ff0000000000L)),
                TimeUuid.NewId(dt0.AddTicks(0x07ff0000000000L)),
                TimeUuid.NewId(dt0.AddTicks(0x07ff0000ff0000L))
            };
            var actual = (TimeUuid[])arr.Clone();
            Array.Sort(actual);
            CollectionAssert.AreEqual(arr, actual);
        }

        [Test]
        public void Parse_Test()
        {
            const string stringUuid = "3d555680-9886-11e4-8101-010101010101";
            var timeuuid = TimeUuid.Parse(stringUuid);
            Assert.AreEqual(TimeUuid.Parse(stringUuid), timeuuid);
            Assert.AreEqual(DateTimeOffset.Parse("2015-01-10 5:05:05 +0"), timeuuid.GetDate());
            Assert.AreEqual(stringUuid, timeuuid.ToString());
        }

        [Test]
        public void Min_Test()
        {
            var timestamp = DateTimeOffset.Now;
            var timeuuid = TimeUuid.Min(timestamp);
            Assert.AreEqual(timestamp, timeuuid.GetDate());
            Assert.AreEqual(new byte[] {0x80, 0x80}, timeuuid.ToByteArray().Skip(8).Take(2));
            Assert.AreEqual(new byte[] {0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, timeuuid.ToByteArray().Skip(10).Take(6));
        }

        [Test]
        public void Max_Test()
        {
            var timestamp = DateTimeOffset.Now;
            var timeuuid = TimeUuid.Max(timestamp);
            Assert.AreEqual(timestamp, timeuuid.GetDate());
            // Variant Byte at index 8: 0x7f is changed into 0xbf
            Assert.AreEqual(new byte[] {0xbf, 0x7f}, timeuuid.ToByteArray().Skip(8).Take(2));
            Assert.AreEqual(new byte[] {0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f}, timeuuid.ToByteArray().Skip(10).Take(6));
        }
    }
}
