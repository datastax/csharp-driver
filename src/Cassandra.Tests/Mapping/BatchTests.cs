using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    /// <summary>
    /// Tests that the Batch execution calls to prepare async and executeasync.
    /// In the case of conditional batches, it tests that also the RowSet is adapted
    /// </summary>
    [TestFixture]
    public class BatchTests : MappingTestBase
    {
        /// <summary>
        /// Gets the mapper for batch statements
        /// </summary>
        private IMapper GetMapper(Func<Task<RowSet>> getRowSetFunc)
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BatchStatement>()))
                .Returns(getRowSetFunc)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            return GetMappingClient(sessionMock);
        }

        [Test]
        public void Execute_Batch_Test()
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(100, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            var batch = mapper.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            mapper.Execute(batch);
        }

        [Test]
        public void Execute_MixedBatch_Test()
        {
            // Generate test user
            const int idx = 20;
            var testUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = Guid.NewGuid();
            Guid updateId = Guid.NewGuid();

            // Create batch of mixed statements and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapper.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            mapper.Execute(batch);
        }

        [Test]
        public void ExecuteConditional_Batch_Empty_RowSet_Test()
        {
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            var batch = mapper.CreateBatch();
            batch.Update<PlainUser>(Cql.New("SET val1 = ? WHERE id = ? IF val2 = ?", "value1", "id1", "value2"));
            batch.Update<PlainUser>(Cql.New("SET val3 = ? WHERE id = ?", "value3", "id1"));
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.True(info.Applied);
        }

        [Test]
        public void ExecuteConditional_Batch_Applied_True_Test()
        {
            var rs = TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]" }, new[] { true });
            var mapper = GetMapper(() => TestHelper.DelayedTask(rs));
            var batch = mapper.CreateBatch();
            batch.InsertIfNotExists(new PlainUser());
            batch.InsertIfNotExists(new PlainUser());
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.True(info.Applied);
            Assert.Null(info.Existing);
        }

        [Test]
        public void ExecuteConditional_Batch_Applied_False_Test()
        {
            var id = Guid.NewGuid();
            var rs = TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]", "userid", "lastlogindate" }, new object[] { false, id, DateTimeOffset.Parse("2015-03-01 +0") });
            var mapper = GetMapper(() => TestHelper.DelayedTask(rs));
            var batch = mapper.CreateBatch();
            batch.Delete<PlainUser>(Cql.New("WHERE userid = ? IF laslogindate = ?", id, DateTimeOffset.Now));
            batch.Insert(new PlainUser());
            var info = mapper.ExecuteConditional<PlainUser>(batch);
            Assert.False(info.Applied);
            Assert.NotNull(info.Existing);
            Assert.AreEqual(id, info.Existing.UserId);
            Assert.AreEqual(DateTimeOffset.Parse("2015-03-01 +0"), info.Existing.LastLoginDate);
        }

        [Test]
        public void ExecuteAsync_Batch_Test()
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(110, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapper.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            mapper.ExecuteAsync(batch);
        }

        [Test]
        public void ExecuteAsync_MixedBatch_Test()
        {
            // Generate test user
            const int idx = 21;
            var testUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = Guid.NewGuid();
            Guid updateId = Guid.NewGuid();

            // Create batch of mixed statements and execute
            var mapper = GetMapper(() => TestHelper.DelayedTask(new RowSet()));
            ICqlBatch batch = mapper.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            mapper.ExecuteAsync(batch).Wait();
        }
    }
}
