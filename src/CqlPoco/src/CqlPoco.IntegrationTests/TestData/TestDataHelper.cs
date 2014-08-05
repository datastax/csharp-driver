using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using CqlPoco.IntegrationTests.Pocos;

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
                            "    lastlogindate timestamp, " +
                            "    loginhistory list<timestamp>, " +
                            "    luckynumbers set<int>, " +
                            "    childrenages map<text, int>, " +
                            "    favoritecolor text, " +
                            "    typeofuser text, " +
                            "    preferredcontactmethod int, " +
                            "    haircolor int, " +
                            "    PRIMARY KEY (userid)" +
                            ")");
        }

        private static void InsertTestData(ISession session)
        {
            // Generate some random users
            List<TestUser> users = Enumerable.Range(1, 10).Select(idx => new TestUser
            {
                UserId = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx%2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContactMethod = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();

            
            foreach (TestUser user in users)
            {
                var insertUser = new SimpleStatement(
                    "INSERT INTO users (userid, name, age, createddate, isactive, lastlogindate, loginhistory, luckynumbers, childrenages, " +
                    "                   favoritecolor, typeofuser, preferredcontactmethod, haircolor) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    .Bind(user.UserId, user.Name, user.Age, user.CreatedDate, user.IsActive, user.LastLoginDate, user.LoginHistory, user.LuckyNumbers, 
                          user.ChildrenAges, user.FavoriteColor.ToString(), user.TypeOfUser == null ? null : user.TypeOfUser.ToString(), 
                          (int) user.PreferredContactMethod, (int?) user.HairColor);

                session.Execute(insertUser);
            }

            Users = users;
        }
    }
}