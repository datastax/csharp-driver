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
                CreatedDate = GetDateTimeInPast(idx),
                IsActive = idx%2 == 0,
                LastLoginDate = GetNullableDateTimeInPast(idx),
                LoginHistory = GetList(idx, GetDateTimeInPast),
                LuckyNumbers = GetSet(idx, i => i),
                ChildrenAges = GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = GetEnumValue<RainbowColor>(idx),
                TypeOfUser = GetEnumValue<UserType?>(idx),
                PreferredContactMethod = GetEnumValue<ContactMethod>(idx),
                HairColor = GetEnumValue<HairColor>(idx)
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

        private static DateTimeOffset GetDateTimeInPast(int index)
        {
            return DateTimeOffset.UtcNow.AddDays(-1*index);
        }

        private static DateTimeOffset? GetNullableDateTimeInPast(int index)
        {
            // Just null out every third record
            if (index%3 == 0)
                return null;

            return GetDateTimeInPast(index);
        }

        private static List<T> GetList<T>(int index, Func<int, T> factory)
        {
            int elementsInList = index % 5;
            if (elementsInList == 0)
                return new List<T>();

            return Enumerable.Range(0, elementsInList).Select(factory).ToList();
        }

        private static HashSet<T> GetSet<T>(int index, Func<int, T> factory)
        {
            int elementsInSet = index % 3;
            if (elementsInSet == 0)
                return new HashSet<T>();

            return new HashSet<T>(Enumerable.Range(0, elementsInSet).Select(factory));
        }

        private static Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(int index, Func<int, TKey> keyFactory, Func<int, TValue> valueFactory)
        {
            int elementsInDictionary = index % 4;
            if (elementsInDictionary == 0)
                return new Dictionary<TKey, TValue>();

            return Enumerable.Range(0, elementsInDictionary).ToDictionary(keyFactory, valueFactory);
        }
        
        private static bool IsNullableType(Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof (Nullable<>);
        }
    }
}