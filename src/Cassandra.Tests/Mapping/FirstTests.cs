using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    /// <summary>
    /// Tests for First and FirstOrDefault methods on client.
    /// </summary>
    public class FirstTests : MappingTestBase
    {
        [Test]
        public void GetFirstAsync_Poco_WithCql()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first user and verify they are same as the user from test data
            var user = mappingClient.FirstAsync<PlainUser>("SELECT * FROM users").Result;
            TestHelper.AssertPropertiesEqual(user, usersExpected.First());
        }

        [Test]
        public void GetFirstAsync_Poco_NoMatch_Throws()
        {
            //Get first user where user id doesn't exist and verify it throws
            //Empty rowset
            var mappingClient = GetMappingClient(new RowSet());
            var ex = Assert.Throws<AggregateException>(() => { mappingClient.FirstAsync<PlainUser>("SELECT * FROM users").Wait(); });
            Assert.IsInstanceOf<InvalidOperationException>(ex.InnerException);
        }

        [Test]
        public void GetFirstOrDefaultAsync_Poco_WithCql()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first or default user and verify they are same as user from test data
            var user = mappingClient.FirstOrDefaultAsync<PlainUser>("SELECT * FROM users").Result;
            TestHelper.AssertPropertiesEqual(usersExpected.First(), user);

            // Get first or default where user id doesn't exist and make sure we get null
            mappingClient = GetMappingClient(new RowSet());
            var notExistingUser = mappingClient.FirstOrDefaultAsync<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty).Result;
            Assert.Null(notExistingUser);
        }

        [Test]
        public void GetFirstOrDefault_Poco_WithCql()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first or default user and verify they are same as user from test data
            var user = mappingClient.FirstOrDefault<PlainUser>("SELECT * FROM users");
            TestHelper.AssertPropertiesEqual(usersExpected.First(), user);

            // Get first or default where user id doesn't exist and make sure we get null
            mappingClient = GetMappingClient(new RowSet());
            var notExistingUser = mappingClient.FirstOrDefault<PlainUser>("SELECT * FROM users WHERE userid = ?", Guid.Empty);
            Assert.Null(notExistingUser);
        }

        [Test]
        public void GetFirstAsync_OneColumnFlattened_WithCql()
        {
            var valueExpected = new DateTimeOffset(635518953563300000, TimeSpan.FromHours(-5));
            var rowset = TestDataHelper.GetSingleValueRowSet("createddate", valueExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first created date and make sure it was one from our test data
            var createdDate = mappingClient.FirstAsync<DateTimeOffset>("SELECT createddate FROM users").Result;
            Assert.AreEqual(valueExpected, createdDate);
        }

        [Test]
        public void GetFirstAsync_OneColumnFlattened_NoMatch_Throws()
        {
            //Get first user where user id doesn't exist and verify it throws
            //Empty rowset
            var mappingClient = GetMappingClient(new RowSet());
            var ex = Assert.Throws<AggregateException>(() => { mappingClient.FirstAsync<Guid>("SELECT userid FROM users").Wait(); });
            Assert.IsInstanceOf<InvalidOperationException>(ex.InnerException);
        }

        [Test]
        public void GetFirst_OneColumnFlattened_WithCql()
        {
            var valueExpected = new DateTimeOffset(635518951562100000, TimeSpan.FromHours(-5));
            var rowset = TestDataHelper.GetSingleValueRowSet("createddate", valueExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first created date and make sure it was one from our test data
            var createdDate = mappingClient.First<DateTimeOffset>("SELECT createddate FROM users");
            Assert.AreEqual(valueExpected, createdDate);
        }

        [Test]
        public void GetFirstOrDefaultAsync_OneColumnFlattened_WithCql()
        {
            const string valueExpected = "hello world, olé utf chars!";
            var rowset = TestDataHelper.GetSingleValueRowSet("name", valueExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first created date and make sure it was one from our test data
            var name = mappingClient.FirstOrDefaultAsync<string>("SELECT name FROM users").Result;
            Assert.AreEqual(valueExpected, name);

            mappingClient = GetMappingClient(new RowSet());
            Assert.Null(
                mappingClient.FirstOrDefaultAsync<List<DateTimeOffset>>("SELECT loginhistory FROM users WHERE userid = ?", Guid.Empty).Result
                );
        }

        [Test]
        public void GetFirstOrDefault_OneColumnFlattened_WithCql()
        {
            const string valueExpected = "hello world, olé utf chars!";
            var rowset = TestDataHelper.GetSingleValueRowSet("name", valueExpected);
            var mappingClient = GetMappingClient(rowset);
            // Get random first created date and make sure it was one from our test data
            var name = mappingClient.FirstOrDefault<string>("SELECT name FROM users");
            Assert.AreEqual(valueExpected, name);

            mappingClient = GetMappingClient(new RowSet());
            Assert.Null(
                mappingClient.FirstOrDefault<List<DateTimeOffset>>("SELECT loginhistory FROM users WHERE userid = ?", Guid.Empty)
                );
        }
    }
}