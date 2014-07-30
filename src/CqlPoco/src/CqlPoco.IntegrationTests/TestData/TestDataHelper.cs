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
        private static readonly ConcurrentDictionary<Type, object[]> EnumValuesCache = new ConcurrentDictionary<Type, object[]>();

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
                            "    typeofuser text, " +
                            "    preferredcontactmethod int, " +
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
                CreatedDate = DateTimeOffset.UtcNow.AddDays(-1*idx),
                IsActive = idx%2 == 0,
                TypeOfUser = GetEnumValue<UserType?>(idx),
                PreferredContactMethod = GetEnumValue<ContactMethod>(idx)
            }).ToList();

            
            foreach (TestUser user in users)
            {
                var insertUser = new SimpleStatement(
                    "INSERT INTO users (userid, name, age, createddate, isactive, typeofuser, preferredcontactmethod) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)")
                    .Bind(user.UserId, user.Name, user.Age, user.CreatedDate, user.IsActive,
                          user.TypeOfUser == null ? null : user.TypeOfUser.ToString(), (int?) user.PreferredContactMethod);

                session.Execute(insertUser);
            }

            Users = users;
        }

        private static TEnum GetEnumValue<TEnum>(int index)
        {
            bool isNullableEnum = IsNullableType(typeof(TEnum));

            // Get the enum type, taking into account nullable enums
            Type enumType = isNullableEnum ? Nullable.GetUnderlyingType(typeof(TEnum)) : typeof (TEnum);
            
            // Get the available enum values
            object[] enumValues = EnumValuesCache.GetOrAdd(enumType, t => Enum.GetValues(enumType).Cast<object>().ToArray());

            // If not a nullable enum, use index with mod to pick an available value
            if (isNullableEnum == false)
                return (TEnum) enumValues[index % enumValues.Length];

            // If a nullable enum, we want to generate null also so treat an index of length + 1 as null
            int idx = index % (enumValues.Length + 1);
            if (idx < enumValues.Length)
                return (TEnum) enumValues[idx];

            return default(TEnum);
        }

        private static bool IsNullableType(Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof (Nullable<>);
        }
    }
}