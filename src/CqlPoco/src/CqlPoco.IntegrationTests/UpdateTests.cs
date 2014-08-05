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
    [TestFixture]
    public class UpdateTests : IntegrationTestBase
    {
        [Test]
        public async void Update_Poco()
        {
            // Get an existing user from the DB
            Guid userId = TestDataHelper.Users[0].UserId;
            var userToUpdate = await CqlClient.Single<UserWithPrimaryKeyDecoration>("WHERE userid = ?", userId);
            userToUpdate.Should().NotBeNull();

            // Change some properties
            userToUpdate.Name = "SomeNewName";
            userToUpdate.Age = 186;
            userToUpdate.CreatedDate = DateTimeOffset.UtcNow.AddHours(-1);
            userToUpdate.IsActive = !userToUpdate.IsActive;
            userToUpdate.LastLoginDate = userToUpdate.LastLoginDate == null ? DateTimeOffset.UtcNow : (DateTimeOffset?) null;
            userToUpdate.LoginHistory.Add(DateTimeOffset.UtcNow);
            userToUpdate.LuckyNumbers.Add(137);
            userToUpdate.ChildrenAges.Add("SomeOtherChild", 5);
            userToUpdate.FavoriteColor = Enum.GetValues(typeof(RainbowColor)).Cast<RainbowColor>().First(v => v != userToUpdate.FavoriteColor);
            userToUpdate.TypeOfUser = userToUpdate.TypeOfUser == null ? UserType.Administrator : (UserType?) null;
            userToUpdate.PreferredContact = Enum.GetValues(typeof(ContactMethod)).Cast<ContactMethod>().First(v => v != userToUpdate.PreferredContact);
            userToUpdate.HairColor = userToUpdate.HairColor == null ? HairColor.Black : (HairColor?) null;

            // Update
            await CqlClient.Update(userToUpdate);

            // Fetch and verify
            var foundUser = await CqlClient.Single<UserWithPrimaryKeyDecoration>("WHERE userid = ?", userId);
            foundUser.ShouldBeEquivalentTo(userToUpdate, opt => opt.AccountForTimestampAccuracy());
        }

        [Test]
        public async void Update_Poco_NoPrimaryKey()
        {
            // Get an existing user from the DB that doesn't have a PK attribute defined on the POCO (i.e. InsertUser class)
            Guid userId = TestDataHelper.Users[1].UserId;
            var userToUpdate = await CqlClient.Single<InsertUser>("WHERE userid = ?", userId);
            userToUpdate.Should().NotBeNull();

            Func<Task> updateUser = async () =>
            {
                await CqlClient.Update(userToUpdate);
            };

            updateUser.ShouldThrow<InvalidOperationException>("no PK was specified and the assumed PK of 'id' is not a column on the POCO");
        }

        [Test]
        public async void Update_Poco_WithCql()
        {
            // Get a user to update
            Guid userId = TestDataHelper.Users[2].UserId;
            var userToUpdate = await CqlClient.Single<UserWithPrimaryKeyDecoration>("WHERE userid = ?", userId);
            userToUpdate.Should().NotBeNull();

            // Modify some values on the user (just so we can assert against it)
            userToUpdate.Name = "SomeNameChangedWithCql";
            userToUpdate.LuckyNumbers.Add(54);
            userToUpdate.FavoriteColor = Enum.GetValues(typeof(RainbowColor)).Cast<RainbowColor>().First(v => v != userToUpdate.FavoriteColor);

            // Update the user using a CQL string (we aren't passing a POCO here, just CQL + params)
            await CqlClient.Update<UserWithPrimaryKeyDecoration>("SET name = ?, luckynumbers = ?, favoritecolor = ? WHERE userid = ?",
                                                                 userToUpdate.Name, userToUpdate.LuckyNumbers, 
                                                                 CqlClient.ConvertCqlArgument<RainbowColor, string>(userToUpdate.FavoriteColor), 
                                                                 userId);

            // Fetch and validate
            var foundUser = await CqlClient.Single<UserWithPrimaryKeyDecoration>("WHERE userid = ?", userId);
            foundUser.ShouldBeEquivalentTo(userToUpdate, opt => opt.AccountForTimestampAccuracy());
        }
    }
}