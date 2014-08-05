using System;
using System.Collections.Generic;
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
        public async void Insert_Poco()
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
    }
}
