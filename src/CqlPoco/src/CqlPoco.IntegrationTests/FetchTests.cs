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
    public class FetchTests : IntegrationTestBase
    {
        [Test]
        public async void FetchAll_Pocos_WithCql()
        {
            List<PlainUser> users = await CqlClient.Fetch<PlainUser>("SELECT * FROM users");
            users.ShouldAllBeEquivalentTo(TestDataHelper.Users, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public async void FetchAll_OneColumnFlattened_WithCql()
        {
            // Try regular value type
            List<int> ages = await CqlClient.Fetch<int>("SELECT age FROM users");
            ages.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.Age).ToList());
            
            // Try nullable type (truncate to ms to account for C* storing timestamps with ms precision)
            List<DateTimeOffset?> lastLogins = await CqlClient.Fetch<DateTimeOffset?>("SELECT lastlogindate FROM users");
            lastLogins.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.LastLoginDate.TruncateToMillisecond()));
            
            // Try string -> enum conversion
            List<RainbowColor> faveColors = await CqlClient.Fetch<RainbowColor>("SELECT favoritecolor FROM users");
            faveColors.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.FavoriteColor));
        }
    }
}