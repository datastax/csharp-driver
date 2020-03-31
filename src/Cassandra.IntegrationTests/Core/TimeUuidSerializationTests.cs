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
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short"), Category("realcluster"), Category("serverapi")]
    public class TimeUuidSerializationTests : SharedClusterTest
    {
        private const string AllTypesTableName = "all_formats_table";
        private const string MinMaxTimeUuidTable = "min_max_timeuuid";
        private PreparedStatement _insertPrepared;
        private PreparedStatement _selectPrepared;

        protected override string[] SetupQueries => new[]
        {
            $"CREATE TABLE {MinMaxTimeUuidTable} (id uuid, timeuuid_sample timeuuid, PRIMARY KEY((id), timeuuid_sample))",
            string.Format(TestUtils.CreateTableAllTypes, AllTypesTableName)
        };
        
        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            var insertQuery = $"INSERT INTO {AllTypesTableName} (id, timeuuid_sample) VALUES (?, ?)";
            var selectQuery = $"SELECT id, timeuuid_sample, dateOf(timeuuid_sample) FROM {AllTypesTableName} WHERE id = ?";
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.GreaterThanOrEqualsTo))
            {
                selectQuery =
                    $"SELECT id, timeuuid_sample, toTimestamp(timeuuid_sample) as timeuuid_date_value FROM {AllTypesTableName} WHERE id = ?";
            }
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
                if (row.GetColumn("timeuuid_date_value") != null)
                {
                    // the timestamp retrieved by the cql function has lower precision than timeuuid
                    const long precision = 10000L;
                    Assert.AreEqual(
                        timeUuid.GetDate().Ticks / precision, 
                        row.GetValue<DateTimeOffset>("timeuuid_date_value").Ticks / precision);
                }
                //Still defaults to Guid
                var boxedValue = row.GetValue<object>("timeuuid_sample");
                Assert.IsInstanceOf<Guid>(boxedValue);
                Assert.AreEqual(resultTimeUuidValue, (TimeUuid)(Guid)boxedValue);
                //The precision is lost, up to milliseconds is fine
                Assert.AreEqual(timeUuid.GetDate().ToString(format), row.GetValue<DateTimeOffset>(2).ToString(format));
            }
        }

        [Test]
        public void TimeUuid_Should_Execute_TimeUuid_CQL_Functions()
        {
            var guid = Guid.NewGuid();
            var timeuuidSample = TimeUuid.NewId();
            var dateOffset = timeuuidSample.GetDate();

            var selectMinMaxTimeuuidPrepared = Session.Prepare($"select * from {MinMaxTimeUuidTable} where id = ? " +
                                                               "and timeuuid_sample < ? and timeuuid_sample > ?");
            var insertMinMaxTimeuuidPrepared = Session.Prepare($"insert into {MinMaxTimeUuidTable} (id, timeuuid_sample) values (?, ?)");

            Session.Execute(insertMinMaxTimeuuidPrepared.Bind(guid, timeuuidSample));
            var row = Session.Execute(selectMinMaxTimeuuidPrepared.Bind(guid, TimeUuid.Max(dateOffset), TimeUuid.Min(dateOffset))).FirstOrDefault();
            Assert.NotNull(row);
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

            var selectQuery = $"SELECT id, timeuuid_sample, dateOf(timeuuid_sample) FROM {AllTypesTableName} LIMIT 10000";
            Assert.DoesNotThrow(() =>
                Session.Execute(selectQuery).Select(r => r.GetValue<TimeUuid>("timeuuid_sample")).ToArray());
        }

        [Test]
        public void ComparisonTest()
        {
            var rowIdBefore = Guid.NewGuid();
            var rowIdAfter = Guid.NewGuid();

            var dt1 = new DateTime(2016, 1, 1, 4, 55, 00);
            var dt2 = new DateTime(2016, 1, 1, 5, 55, 00);
            var ctimes = dt1.CompareTo(dt2);
            
            //base check
            Assert.That(ctimes < 0);

            var timeuuidBefore = TimeUuid.NewId(dt1);
            var timeuuidAfter = TimeUuid.NewId(dt2);

            Session.Execute(_insertPrepared.Bind(rowIdBefore, timeuuidBefore));
            Session.Execute(_insertPrepared.Bind(rowIdAfter, timeuuidAfter));

            var cuuids = timeuuidBefore.CompareTo(timeuuidAfter);
            Assert.That(cuuids < 0); //Double checking Timeuuid comparison

            var row1 = Session.Execute(_selectPrepared.Bind(rowIdBefore)).FirstOrDefault();
            Assert.NotNull(row1);
            var row2 = Session.Execute(_selectPrepared.Bind(rowIdAfter)).FirstOrDefault();
            Assert.NotNull(row2);

            if (row1.GetColumn("timeuuid_date_value") != null
                && row2.GetColumn("timeuuid_date_value") != null)
            {
                //checking the comparison of (de)serialized timeuuid timestamps
                var ticks1 = row1.GetValue<DateTimeOffset>("timeuuid_date_value").Ticks;
                var ticks2 = row2.GetValue<DateTimeOffset>("timeuuid_date_value").Ticks;
                Assert.Greater(ticks2, ticks1);
            }
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
