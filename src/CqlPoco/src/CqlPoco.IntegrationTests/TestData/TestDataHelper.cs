using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;

namespace CqlPoco.IntegrationTests.TestData
{
    /// <summary>
    /// Helper class for managing test data.
    /// </summary>
    public static class TestDataHelper
    {
        /// <summary>
        /// An in-memory representation of the users test data that should be in C*.
        /// </summary>
        public static List<TestUser> Users { get; private set; }

        /// <summary>
        /// Drops and recreates the schema and inserts test data.
        /// </summary>
        public static void ResetTestData()
        {
            ISession session = SessionHelper.Session;
            DropAndRecreateSchema(session);
            InsertTestData(session);
        }

        private static void DropAndRecreateSchema(ISession session)
        {
            // Drop any existing tables
            session.Execute("DROP TABLE IF EXISTS users");
            
            // Create tables
            session.Execute("CREATE TABLE users (" +
                            "    userid uuid, " +
                            "    name text, " +
                            "    age int, " +
                            "    createddate timestamp, " +
                            "    isactive boolean, " +
                            "    PRIMARY KEY (userid)" +
                            ")");
        }

        private static void InsertTestData(ISession session)
        {
            List<TestUser> users = Enumerable.Range(1, 10).Select(idx => new TestUser
            {
                UserId = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = DateTimeOffset.UtcNow.AddDays(-1*idx),
                IsActive = idx%2 == 0
            }).ToList();

            
            foreach (TestUser user in users)
            {
                var insertUser = new SimpleStatement("INSERT INTO users (userid, name, age, createddate, isactive) VALUES (?, ?, ?, ?, ?)")
                    .Bind(user.UserId, user.Name, user.Age, user.CreatedDate, user.IsActive);

                session.Execute(insertUser);
            }

            Users = users;
        }
    }
}