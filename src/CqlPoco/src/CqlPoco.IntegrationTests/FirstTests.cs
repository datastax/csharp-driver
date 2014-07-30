using System;
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
        public async void GetFirstWithCql()
        {
            // Get random first user and verify they are same as the user from test data
            PlainUser user = await CqlClient.First<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first user where user id doesn't exist and verify it throws
            Func<Task> getUser = async () =>
            {
                PlainUser notExistingUser = await CqlClient.First<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty).ConfigureAwait(false);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }

        [Test]
        public async void GetFirstOrDefaultWithCql()
        {
            // Get random first or default user and verify they are same as user from test data
            PlainUser user = await CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users");
            user.ShouldBeEquivalentTo(TestDataHelper.Users.Single(u => u.UserId == user.UserId), opt => opt.AccountForTimestampAccuracy());

            // Get first or default where user id doesn't exist and make sure we get null
            PlainUser notExistingUser = await CqlClient.FirstOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }
    }
}