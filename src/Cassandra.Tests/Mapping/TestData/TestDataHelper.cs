using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Tests.Mapping.Pocos;

namespace Cassandra.Tests.Mapping.TestData
{
    internal static class TestDataHelper
    {
        private const int ProtocolVersion = 2;

        public static List<PlainUser> GetUserList()
        {
            // Generate some random users
            return Enumerable.Range(1, 10).Select(idx => new PlainUser
            {
                UserId = Guid.NewGuid(),
                Name = string.Format("Name {0}", idx),
                Age = idx,
                CreatedDate = TestDataGenerator.GetDateTimeInPast(idx),
                IsActive = idx % 2 == 0,
                LastLoginDate = TestDataGenerator.GetNullableDateTimeInPast(idx),
                LoginHistory = TestDataGenerator.GetList(idx, TestDataGenerator.GetDateTimeInPast),
                LuckyNumbers = TestDataGenerator.GetSet(idx, i => i),
                ChildrenAges = TestDataGenerator.GetDictionary(idx, i => string.Format("Child {0}", i), i => i),
                FavoriteColor = TestDataGenerator.GetEnumValue<RainbowColor>(idx),
                TypeOfUser = TestDataGenerator.GetEnumValue<UserType?>(idx),
                PreferredContactMethod = TestDataGenerator.GetEnumValue<ContactMethod>(idx),
                HairColor = TestDataGenerator.GetEnumValue<HairColor>(idx)
            }).ToList();
        }

        public static RowSet GetUsersRowSet(IEnumerable<PlainUser> users)
        {
            var rs = new RowSet();
            rs.Columns = new []
            {
                new CqlColumn { Name = "userid", TypeCode = ColumnTypeCode.Uuid, Type = typeof(Guid), Index = 0},
                new CqlColumn { Name = "name", TypeCode = ColumnTypeCode.Text, Type = typeof(string), Index = 1},
                new CqlColumn { Name = "age", TypeCode = ColumnTypeCode.Int, Type = typeof(int), Index = 2},
                new CqlColumn { Name = "createddate", TypeCode = ColumnTypeCode.Timestamp, Type = typeof(DateTimeOffset), Index = 3},
                new CqlColumn { Name = "isactive", TypeCode = ColumnTypeCode.Boolean, Type = typeof(bool), Index = 4},
                new CqlColumn { Name = "lastlogindate", TypeCode = ColumnTypeCode.Timestamp, Type = typeof(DateTimeOffset), Index = 5},
                new CqlColumn { Name = "loginhistory", TypeCode = ColumnTypeCode.List, TypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Timestamp}, Type = typeof(List<DateTimeOffset>), Index = 6},
                new CqlColumn { Name = "luckynumbers", TypeCode = ColumnTypeCode.Set, TypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int}, Type = typeof(HashSet<int>), Index = 7},
                new CqlColumn { Name = "childrenages", TypeCode = ColumnTypeCode.Map, TypeInfo = new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int}, Type = typeof(IDictionary<string, int>), Index = 8},
                new CqlColumn { Name = "favoritecolor", TypeCode = ColumnTypeCode.Int, Type = typeof(int), Index = 9},
                new CqlColumn { Name = "typeofuser", TypeCode = ColumnTypeCode.Int, Type = typeof(int), Index = 10},
                new CqlColumn { Name = "preferredcontactmethod", TypeCode = ColumnTypeCode.Int, Type = typeof(int), Index = 11},
                new CqlColumn { Name = "haircolor", TypeCode = ColumnTypeCode.Int, Type = typeof(int), Index = 12},
            };
            var columnIndexes = rs.Columns.ToDictionary(c => c.Name, c => c.Index);
            foreach (var user in users)
            {
                var values = new List<object>
                {
                    user.UserId,
                    user.Name,
                    user.Age,
                    user.CreatedDate,
                    user.IsActive,
                    user.LastLoginDate,
                    user.LoginHistory,
                    user.LuckyNumbers,
                    user.ChildrenAges,
                    (int)user.FavoriteColor,
                    (int?)user.TypeOfUser,
                    (int)user.PreferredContactMethod,
                    (int?)user.HairColor
                }.Select(v => TypeCodec.Encode(ProtocolVersion, v));
                var row = new Row(ProtocolVersion, values.ToArray(), rs.Columns, columnIndexes);
                rs.AddRow(row);
            }
            return rs;
        }
    }
}
