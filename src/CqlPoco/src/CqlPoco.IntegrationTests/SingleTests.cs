using System;
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
        public async void GetSingleWithCql()
        {
            // Get a user that exists
            PlainUser user = await CqlClient.Single<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[0].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[0], opt => opt.AccountForTimestampAccuracy());

            // Get a user that shouldn't exist (using Guid.Empty as the Id)
            Func<Task> getUser = async () =>
            {
                PlainUser notExistingUser = await CqlClient.Single<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty)
                                                           .ConfigureAwait(false);
            };
            getUser.ShouldThrow<InvalidOperationException>();
        }
        
        [Test]
        public async void GetSingleOrDefaultWithCql()
        {
            // Get a user that exists
            PlainUser user = await CqlClient.SingleOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", TestDataHelper.Users[1].UserId);
            user.ShouldBeEquivalentTo(TestDataHelper.Users[1], opt => opt.AccountForTimestampAccuracy());

            // Get a user that doesn't exist (using Guid.Empty as the Id)
            PlainUser notExistingUser = await CqlClient.SingleOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            notExistingUser.Should().BeNull();
        }
    }
}