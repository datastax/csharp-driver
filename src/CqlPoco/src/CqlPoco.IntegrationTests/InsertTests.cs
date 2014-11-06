using System;
using System.Linq;
using CqlPoco.IntegrationTests.Assertions;
using CqlPoco.IntegrationTests.Pocos;
using CqlPoco.IntegrationTests.TestData;
using FluentAssertions;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    [TestFixture]
    public class InsertTests : IntegrationTestBase
    {
        [Test]
        public async void InsertAsync_Poco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            TestUser user = TestDataHelper.Users.First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = user.ChildrenAges,
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            // Insert the new user
            await CqlClient.InsertAsync(newUser);

            // Fetch and verify
            var foundUser = await CqlClient.SingleAsync<InsertUser>("WHERE userid = ?", newUser.Id);
            foundUser.ShouldBeEquivalentTo(newUser, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public void Insert_Poco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            TestUser user = TestDataHelper.Users.First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = user.ChildrenAges,
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            // Insert the new user
            CqlClient.Insert(newUser);

            // Fetch and verify
            var foundUser = CqlClient.Single<InsertUser>("WHERE userid = ?", newUser.Id);
            foundUser.ShouldBeEquivalentTo(newUser, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public async void InsertAsync_FluentPoco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            TestUser user = TestDataHelper.Users.First();
            var newUser = new FluentUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = user.ChildrenAges,
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            // Insert the new user
            await CqlClient.InsertAsync(newUser);

            // Fetch and verify
            var foundUser = await CqlClient.SingleAsync<FluentUser>("WHERE userid = ?", newUser.Id);
            foundUser.ShouldBeEquivalentTo(newUser, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public void Insert_FluentPoco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            TestUser user = TestDataHelper.Users.First();
            var newUser = new FluentUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = user.ChildrenAges,
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            // Insert the new user
            CqlClient.Insert(newUser);

            // Fetch and verify
            var foundUser = CqlClient.Single<FluentUser>("WHERE userid = ?", newUser.Id);
            foundUser.ShouldBeEquivalentTo(newUser, opt => opt.AccountForTimestampAccuracy());
        }
    }
}
