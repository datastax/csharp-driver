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
        public async void GetFirst_Poco_WithCql()
        {
            // Get random first user and verify they are same as the user from test data
            var user = await CqlClient.First<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first user where user id doesn't exist and verify it throws
            Func<Task> getUser = async () =>
            {
                var notExistingUser = await CqlClient.First<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty).ConfigureAwait(false);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetFirstOrDefault_Poco_WithCql()
        {
            // Get random first or default user and verify they are same as user from test data
            var user = await CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first or default where user id doesn't exist and make sure we get null
            var notExistingUser = await CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }

        [Test]
        public async void GetFirst_OneColumnFlattened_WithCql()
        {
            // Get random first created date and make sure it was one from our test data
            var createdDate = await CqlClient.First<DateTimeOffset>("SELECT createddate FROM users");
            TestDataHelper.Users.Select(u => u.CreatedDate.TruncateToMillisecond()).Should().Contain(createdDate);

            // Verify getting random first for user that doesn't exist throws
            Func<Task> getUserId = async () =>
            {
                var userId = await CqlClient.First<Guid>("SELECT userid FROM users WHERE userid = ?", Guid.Empty).ConfigureAwait(false);
            };
            getUserId.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetFirstOrDefault_OneColumnFlattened_WithCql()
        {
            // Get random first or default name and make sure it was one from our test data
            var name = await CqlClient.FirstOrDefault<string>("SELECT name FROM users");
            TestDataHelper.Users.Select(u => u.Name).Should().Contain(name);

            // Get random first or default login history for user that does not exist and make sure we get null
            var loginHistory = await CqlClient.FirstOrDefault<List<DateTimeOffset>>("SELECT loginhistory FROM users WHERE userid = ?", Guid.Empty);
            loginHistory.Should().BeNull();
        }
    }
}