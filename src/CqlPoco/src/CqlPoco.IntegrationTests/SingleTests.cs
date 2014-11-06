using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CqlPoco.IntegrationTests.Assertions;
using CqlPoco.IntegrationTests.Pocos;
using CqlPoco.IntegrationTests.TestData;
using FluentAssertions;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    [TestFixture]
    public class SingleTests : IntegrationTestBase
    {
        [Test]
        public async void GetSingleAsync_Poco_WithCql()
        {
            // Get a user that exists
            var user = await CqlClient.SingleAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[0].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[0], opt => opt.AccountForTimestampAccuracy());

            // Get a user that shouldn't exist (using Guid.Empty as the Id)
            Func<Task> getUser = async () =>
            {
                var notExistingUser = await CqlClient.SingleAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty)
                                                     .ConfigureAwait(false);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public void GetSingle_Poco_WithCql()
        {
            // Get a user that exists
            var user = CqlClient.Single<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[0].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[0], opt => opt.AccountForTimestampAccuracy());

            // Get a user that shouldn't exist (using Guid.Empty as the Id)
            Action getUser = () =>
            {
                var notExistingUser = CqlClient.Single<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }
        
        [Test]
        public async void GetSingleOrDefaultAsync_Poco_WithCql()
        {
            // Get a user that exists
            var user = await CqlClient.SingleOrDefaultAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[1].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[1], opt => opt.AccountForTimestampAccuracy());

            // Get a user that doesn't exist (using Guid.Empty as the Id)
            var notExistingUser = await CqlClient.SingleOrDefaultAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }

        [Test]
        public void GetSingleOrDefault_Poco_WithCql()
        {
            // Get a user that exists
            var user = CqlClient.SingleOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[1].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[1], opt => opt.AccountForTimestampAccuracy());

            // Get a user that doesn't exist (using Guid.Empty as the Id)
            var notExistingUser = CqlClient.SingleOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }

        [Test]
        public async void GetSingleAsync_OneColumnFlattened_WithCql()
        {
            // Get the name for a user that exists
            var name = await CqlClient.SingleAsync<string>("SELECT name FROM users WHERE userid = ?", TestDataHelper.Users[2].UserId);
            name.Should().Be(TestDataHelper.Users[2].Name);

            // Get the type of user for a user that doesn't exist
            Func<Task> getUserType = async () =>
            {
                var userType = await CqlClient.SingleAsync<UserType?>("SELECT typeofuser FROM users WHERE userid = ?", Guid.Empty)
                                              .ConfigureAwait(false);
            };
            getUserType.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public void GetSingle_OneColumnFlattened_WithCql()
        {
            // Get the name for a user that exists
            var name = CqlClient.Single<string>("SELECT name FROM users WHERE userid = ?", TestDataHelper.Users[2].UserId);
            name.Should().Be(TestDataHelper.Users[2].Name);

            // Get the type of user for a user that doesn't exist
            Action getUserType = () =>
            {
                var userType = CqlClient.Single<UserType?>("SELECT typeofuser FROM users WHERE userid = ?", Guid.Empty);
            };
            getUserType.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetSingleOrDefaultAsync_OneColumnFlattened_WithCql()
        {
            // Get the lucky numbers for a user that exists
            var luckyNumbers =
                await CqlClient.SingleOrDefaultAsync<HashSet<int>>("SELECT luckynumbers FROM users WHERE userid = ?", TestDataHelper.Users[1].UserId);
            luckyNumbers.Should().BeEquivalentTo(TestDataHelper.Users[1].LuckyNumbers);

            // Get IsActive for a user that doesn't exist
            var isActive = await CqlClient.SingleOrDefaultAsync<bool?>("SELECT isactive FROM users WHERE userid = ?", Guid.Empty);
            isActive.Should().NotHaveValue();
        }

        [Test]
        public void GetSingleOrDefault_OneColumnFlattened_WithCql()
        {
            // Get the lucky numbers for a user that exists
            var luckyNumbers = CqlClient.SingleOrDefault<HashSet<int>>("SELECT luckynumbers FROM users WHERE userid = ?",
                                                                       TestDataHelper.Users[1].UserId);
            luckyNumbers.Should().BeEquivalentTo(TestDataHelper.Users[1].LuckyNumbers);

            // Get IsActive for a user that doesn't exist
            var isActive = CqlClient.SingleOrDefault<bool?>("SELECT isactive FROM users WHERE userid = ?", Guid.Empty);
            isActive.Should().NotHaveValue();
        }
    }
}