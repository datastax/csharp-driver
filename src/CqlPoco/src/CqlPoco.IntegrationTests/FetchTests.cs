using System.Collections.Generic;
using CqlPoco.IntegrationTests.Assertions;
using CqlPoco.IntegrationTests.TestData;
using FluentAssertions;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    [TestFixture]
    public class FetchTests : IntegrationTestBase
    {
        [Test]
        public async void FetchAllCql()
        {
            List<PlainUser> users = await CqlClient.Fetch<PlainUser>("SELECT * FROM users");
            users.Count.Should().Be(TestDataHelper.Users.Count);
            users.ShouldAllBeEquivalentTo(TestDataHelper.Users, opt => opt.AccountForTimestampAccuracy());
        }
    }
}