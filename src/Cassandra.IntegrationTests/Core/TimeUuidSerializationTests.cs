using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class TimeUuidSerializationTests : SingleNodeClusterTest
    {
        private const string Keyspace = "ks1";
        private const string Table = Keyspace + ".tbl1";
        private PreparedStatement _insertPrepared;
        private PreparedStatement _selectPrepared;


        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();
            Session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, Keyspace, 1));
            Session.Execute(String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, Table));
            var insertQuery = String.Format("INSERT INTO {0} (id, timeuuid_sample) VALUES (?, ?)", Table);
            var selectQuery = String.Format("SELECT id, timeuuid_sample, dateOf(timeuuid_sample) FROM {0} WHERE id = ?", Table);
            _insertPrepared = Session.Prepare(insertQuery);
            _selectPrepared = Session.Prepare(selectQuery);
        }

        [Test]
        public void DeserializationTests()
        {
            var rowIdArray = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            var ticksArray = new[] { 35465410426481591, 63546500026481590, 70011723893288L, 1723893288L };
            const string format = "yyyy-MM-dd HH:mm:ss.SSS";
            for (var i = 0; i < rowIdArray.Length; i++)
            {
                var rowId = rowIdArray[i];
                var ticks = ticksArray[i];
                var timeUuid = TimeUuid.NewId(new DateTimeOffset(ticks, TimeSpan.Zero));
                Session.Execute(_insertPrepared.Bind(rowId, timeUuid));
                var row = Session.Execute(_selectPrepared.Bind(rowId)).FirstOrDefault();
                Assert.NotNull(row);
                var resultTimeUuidValue = row.GetValue<TimeUuid>("timeuuid_sample");
                Assert.AreEqual(timeUuid, resultTimeUuidValue);
                Assert.AreEqual(timeUuid.GetDate(), resultTimeUuidValue.GetDate());
                //Still defaults to Guid
                var boxedValue = row.GetValue<object>("timeuuid_sample");
                Assert.IsInstanceOf<Guid>(boxedValue);
                Assert.AreEqual(resultTimeUuidValue, (TimeUuid)(Guid)boxedValue);
                //The precision is lost, up to milliseconds is fine
                Assert.AreEqual(timeUuid.GetDate().ToString(format), row.GetValue<DateTimeOffset>("dateOf(timeuuid_sample)").ToString(format));
            }
        }

        [Test]
        public void RandomValuesTest()
        {
            var tasks = new List<Task>();
            for (var i = 0; i < 500; i++)
            {
                tasks.Add(
                    Session.ExecuteAsync(_insertPrepared.Bind(Guid.NewGuid(), TimeUuid.NewId())));
            }
            Assert.DoesNotThrow(() => Task.WaitAll(tasks.ToArray()));

            var selectQuery = String.Format("SELECT id, timeuuid_sample, dateOf(timeuuid_sample) FROM {0} LIMIT 10000", Table);
            Assert.DoesNotThrow(() =>
                Session.Execute(selectQuery).Select(r => r.GetValue<TimeUuid>("timeuuid_sample")).ToArray());
        }

        [Test]
        public void SerializationTests()
        {
            //TimeUuid and Guid are valid values for a timeuuid column value
            Assert.DoesNotThrow(() =>
                Session.Execute(_insertPrepared.Bind(Guid.NewGuid(), TimeUuid.NewId())));

            var validUuidV1Bytes = TimeUuid.NewId().ToByteArray();
            Assert.DoesNotThrow(() =>
                Session.Execute(_insertPrepared.Bind(Guid.NewGuid(), new Guid(validUuidV1Bytes))));
        }
    }
}
