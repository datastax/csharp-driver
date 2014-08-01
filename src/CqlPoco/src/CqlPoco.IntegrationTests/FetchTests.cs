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
        public async void FetchAll_Pocos_WithPredicateOnly()
        {
            // Lookup the first 2 users by id with only a FROM + WHERE
            TestUser[] usersToGet = TestDataHelper.Users.Take(2).ToArray();
            List<PlainUser> users = await CqlClient.Fetch<PlainUser>("FROM users WHERE userid IN (?, ?)", usersToGet[0].UserId, usersToGet[1].UserId);
            users.ShouldAllBeEquivalentTo(usersToGet, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public async void FetchAll_DecoratedPocos_WithNoCql()
        {
            // We should be able to get all the decorated POCOs without any CQL (i.e. have it generated for us)
            List<DecoratedUser> users = await CqlClient.Fetch<DecoratedUser>();
            foreach (DecoratedUser user in users)
            {
                // Match users from UserId -> Id property and test that matching properties are equivalent
                TestUser testUser = TestDataHelper.Users.SingleOrDefault(u => u.UserId == user.Id);
                user.ShouldBeEquivalentTo(testUser, opt => opt.ExcludingMissingProperties());

                // Also make sure that ignored property was ignored
                user.AnUnusedProperty.Should().NotHaveValue();
            }
        }

        [Test]
        public async void FetchAll_DecoratedPocos_WithCql()
        {
            List<DecoratedUser> users = await CqlClient.Fetch<DecoratedUser>("SELECT * FROM users");
            foreach (DecoratedUser user in users)
            {
                // Match users from UserId -> Id property and test that matching properties are equivalent
                TestUser testUser = TestDataHelper.Users.SingleOrDefault(u => u.UserId == user.Id);
                user.ShouldBeEquivalentTo(testUser, opt => opt.ExcludingMissingProperties());

                // Also make sure that ignored property was ignored
                user.AnUnusedProperty.Should().NotHaveValue();
            }
        }

        [Test]
        public async void FetchAll_DecoratedPocos_WithPredicateOnly()
        {
            // Lookup users 3 and 4 with just a WHERE clause
            TestUser[] usersToGet = TestDataHelper.Users.Skip(2).Take(2).ToArray();
            List<DecoratedUser> users = await CqlClient.Fetch<DecoratedUser>("WHERE userid IN (?, ?)", usersToGet[0].UserId, usersToGet[1].UserId);

            foreach (DecoratedUser user in users)
            {
                // Match users from UserId -> Id property and test that matching properties are equivalent
                TestUser testUser = usersToGet.SingleOrDefault(u => u.UserId == user.Id);
                user.ShouldBeEquivalentTo(testUser, opt => opt.ExcludingMissingProperties());

                // Also make sure that ignored property was ignored
                user.AnUnusedProperty.Should().NotHaveValue();
            }
        }

        [Test]
        public async void FetchAll_ExplicitColumnsPocos_WithNoCql()
        {
            // We should be able to fetch explicit columns poco with no CQL (i.e. have it generated for us)
            List<ExplicitColumnsUser> users = await CqlClient.Fetch<ExplicitColumnsUser>();

            // Compare to test users but exclude missing properties since we only queried a subset, as well as explicitly ignore
            // the name property because that should not have been mapped because it's missing a Column attribute
            users.ShouldAllBeEquivalentTo(TestDataHelper.Users, opt => opt.ExcludingMissingProperties().Excluding(u => u.Name));

            // All name properties should be null
            users.Select(u => u.Name).Should().OnlyContain(name => name == null);
        }

        [Test]
        public async void FetchAll_ExplicitColumnsPocos_WithCql()
        {
            List<ExplicitColumnsUser> users = await CqlClient.Fetch<ExplicitColumnsUser>("SELECT * FROM users");

            // Compare to test users but exclude missing properties since we only queried a subset, as well as explicitly ignore
            // the name property because that should not have been mapped because it's missing a Column attribute
            users.ShouldAllBeEquivalentTo(TestDataHelper.Users, opt => opt.ExcludingMissingProperties().Excluding(u => u.Name));

            // All name properties should be null
            users.Select(u => u.Name).Should().OnlyContain(name => name == null);
        }

        [Test]
        public async void FetchAll_ExplicitColumnsPocos_WithPredicateOnly()
        {
            // Lookup users 5 and 6 with just a WHERE clause
            TestUser[] usersToGet = TestDataHelper.Users.Skip(4).Take(2).ToArray();

            List<ExplicitColumnsUser> users = await CqlClient.Fetch<ExplicitColumnsUser>("WHERE userid IN (?, ?)", usersToGet[0].UserId, usersToGet[1].UserId);

            // Compare to test users but exclude missing properties since we only queried a subset, as well as explicitly ignore
            // the name property because that should not have been mapped because it's missing a Column attribute
            users.ShouldAllBeEquivalentTo(usersToGet, opt => opt.ExcludingMissingProperties().Excluding(u => u.Name));

            // All name properties should be null
            users.Select(u => u.Name).Should().OnlyContain(name => name == null);
        }

        [Test]
        public async void FetchAll_OneColumnFlattened_WithCql()
        {
            // Try regular value type
            List<int> ages = await CqlClient.Fetch<int>("SELECT age FROM users");
            ages.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.Age).ToList());
            
            // Try nullable type (truncate to ms to account for C* storing timestamps with ms precision)
            List<DateTimeOffset?> lastLogins = await CqlClient.Fetch<DateTimeOffset?>("SELECT lastlogindate FROM users");
            lastLogins.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.LastLoginDate.ToMillisecondPrecision()));
            
            // Try string -> enum conversion
            List<RainbowColor> faveColors = await CqlClient.Fetch<RainbowColor>("SELECT favoritecolor FROM users");
            faveColors.Should().BeEquivalentTo(TestDataHelper.Users.Select(u => u.FavoriteColor));
        }
    }
}