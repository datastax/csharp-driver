using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CqlPoco.IntegrationTests.Assertions;
using CqlPoco.IntegrationTests.Pocos;
using CqlPoco.IntegrationTests.TestData;
using FluentAssertions;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    /// <summary>
    /// Tests for First and FirstOrDefault methods on client.
    /// </summary>
    [TestFixture]
    public class FirstTests : IntegrationTestBase
    {
        [Test]
        public async void GetFirstAsync_Poco_WithCql()
        {
            // Get random first user and verify they are same as the user from test data
            var user = await CqlClient.FirstAsync<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first user where user id doesn't exist and verify it throws
            Func<Task> getUser = async () =>
            {
                var notExistingUser = await CqlClient.FirstAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty).ConfigureAwait(false);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public void GetFirst_Poco_WithCql()
        {
            // Get random first user and verify they are same as the user from test data
            var user = CqlClient.First<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first user where user id doesn't exist and verify it throws
            Action getUser = () =>
            {
                var notExistingUser = CqlClient.First<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetFirstOrDefaultAsync_Poco_WithCql()
        {
            // Get random first or default user and verify they are same as user from test data
            var user = await CqlClient.FirstOrDefaultAsync<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first or default where user id doesn't exist and make sure we get null
            var notExistingUser = await CqlClient.FirstOrDefaultAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }

        [Test]
        public void GetFirstOrDefault_Poco_WithCql()
        {
            // Get random first or default user and verify they are same as user from test data
            var user = CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first or default where user id doesn't exist and make sure we get null
            var notExistingUser = CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }

        [Test]
        public async void GetFirstAsync_OneColumnFlattened_WithCql()
        {
            // Get random first created date and make sure it was one from our test data
            var createdDate = await CqlClient.FirstAsync<DateTimeOffset>("SELECT createddate FROM users");
            TestDataHelper.Users.Select(u => u.CreatedDate.ToMillisecondPrecision()).Should().Contain(createdDate);

            // Verify getting random first for user that doesn't exist throws
            Func<Task> getUserId = async () =>
            {
                var userId = await CqlClient.FirstAsync<Guid>("SELECT userid FROM users WHERE userid = ?", Guid.Empty).ConfigureAwait(false);
            };
            getUserId.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public void GetFirst_OneColumnFlattened_WithCql()
        {
            // Get random first created date and make sure it was one from our test data
            var createdDate = CqlClient.First<DateTimeOffset>("SELECT createddate FROM users");
            TestDataHelper.Users.Select(u => u.CreatedDate.ToMillisecondPrecision()).Should().Contain(createdDate);

            // Verify getting random first for user that doesn't exist throws
            Action getUserId = () =>
            {
                var userId = CqlClient.First<Guid>("SELECT userid FROM users WHERE userid = ?", Guid.Empty);
            };
            getUserId.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetFirstOrDefaultAsync_OneColumnFlattened_WithCql()
        {
            // Get random first or default name and make sure it was one from our test data
            var name = await CqlClient.FirstOrDefaultAsync<string>("SELECT name FROM users");
            TestDataHelper.Users.Select(u => u.Name).Should().Contain(name);

            // Get random first or default login history for user that does not exist and make sure we get null
            var loginHistory = await CqlClient.FirstOrDefaultAsync<List<DateTimeOffset>>("SELECT loginhistory FROM users WHERE userid = ?", Guid.Empty);
            loginHistory.Should().BeNull();
        }

        [Test]
        public void GetFirstOrDefault_OneColumnFlattened_WithCql()
        {
            // Get random first or default name and make sure it was one from our test data
            var name = CqlClient.FirstOrDefault<string>("SELECT name FROM users");
            TestDataHelper.Users.Select(u => u.Name).Should().Contain(name);

            // Get random first or default login history for user that does not exist and make sure we get null
            var loginHistory = CqlClient.FirstOrDefault<List<DateTimeOffset>>("SELECT loginhistory FROM users WHERE userid = ?", Guid.Empty);
            loginHistory.Should().BeNull();
        }
    }
}