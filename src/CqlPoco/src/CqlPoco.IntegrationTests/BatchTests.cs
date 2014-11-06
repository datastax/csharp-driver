using System;
using System.Linq;
using CqlPoco.IntegrationTests.Pocos;
using CqlPoco.IntegrationTests.TestData;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    [TestFixture]
    public class BatchTests : IntegrationTestBase
    {
        [Test]
        public void Execute_Batch()
        {
            // Generate 3 test users
            var testUsers = Enumerable.Range(100, 3).Select(idx => new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx%2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            ICqlBatch batch = CqlClient.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            CqlClient.Execute(batch);
        }

        [Test]
        public void Execute_MixedBatch()
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
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = TestDataHelper.Users[0].UserId;
            Guid updateId = TestDataHelper.Users[1].UserId;

            // Create batch of mixed statements and execute
            ICqlBatch batch = CqlClient.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            CqlClient.Execute(batch);
        }

        [Test]
        public async void ExecuteAsync_Batch()
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
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            // Create batch to insert users and execute
            ICqlBatch batch = CqlClient.CreateBatch();
            batch.Insert(testUsers[0]);
            batch.Insert(testUsers[1]);
            batch.Insert(testUsers[2]);
            await CqlClient.ExecuteAsync(batch);
        }

        [Test]
        public async void ExecuteAsync_MixedBatch()
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
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContact = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            };

            // Get id of existing user for deleting and updating
            Guid deleteId = TestDataHelper.Users[2].UserId;
            Guid updateId = TestDataHelper.Users[3].UserId;

            // Create batch of mixed statements and execute
            ICqlBatch batch = CqlClient.CreateBatch();
            batch.Insert(testUser);
            batch.Delete<InsertUser>("WHERE userid = ?", deleteId);
            batch.Update<InsertUser>("SET name = ? WHERE userid = ?", "SomeNewName", updateId);
            await CqlClient.ExecuteAsync(batch);
        }
    }
}
