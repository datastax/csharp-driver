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
                new CqlColumn { Name = "userid", TypeCode = ColumnTypeCode.Uuid, Type = typeof(Guid), Index = 0}
            };
            var columnIndexes = rs.Columns.ToDictionary(c => c.Name, c => c.Index);
            foreach (var user in users)
            {
                var values = new List<object>
                {
                    user.UserId
                }.Select(v => TypeCodec.Encode(ProtocolVersion, v));
                var row = new Row(ProtocolVersion, values.ToArray(), rs.Columns, columnIndexes);
                rs.AddRow(row);
            }
            return rs;
        }
    }
}
