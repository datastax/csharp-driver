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

        public static List<PlainUser> GetUserList(int length = 10)
        {
            // Generate some random users
            return Enumerable.Range(1, length).Select(idx => new PlainUser
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

        /// <summary>
        /// Returns a RowSet with 1 column
        /// </summary>
        public static RowSet GetSingleColumnRowSet<T>(string columnName, T[] values)
        {
            var rs = new RowSet();
            IColumnInfo typeInfo;
            var typeCode = TypeCodec.GetColumnTypeCodeInfo(typeof (T), out typeInfo);
            rs.Columns = new[]
            {
                new CqlColumn { Name = columnName, TypeCode = typeCode, TypeInfo = typeInfo, Type = typeof(T), Index = 0}
            };
            var columnIndexes = rs.Columns.ToDictionary(c => c.Name, c => c.Index);

            foreach (var v in values)
            {
                var row = new Row(new object[] { v}, rs.Columns, columnIndexes);
                rs.AddRow(row);
            }
            return rs;
        }

        /// <summary>
        /// Returns a RowSet with a single column and row
        /// </summary>
        public static RowSet GetSingleValueRowSet<T>(string columnName, T value)
        {
            return GetSingleColumnRowSet(columnName, new T[] { value });
        }

        public static RowSet CreateMultipleValuesRowSet<T>(string[] columnNames, T[] genericValues, int rowLength = 1)
        {
            var rs = new RowSet();
            rs.Columns = new CqlColumn[columnNames.Length];
            for (var i = 0; i < columnNames.Length; i++)
            {
                IColumnInfo typeInfo;
                var type = typeof (T);
                if (type == typeof (Object))
                {
                    //Try to guess by value
                    if (genericValues[i] == null)
                    {
                        throw new Exception("Test data could not be generated, value at index " + i + " could not be encoded");
                    }
                    type = genericValues[i].GetType();
                }
                var typeCode = TypeCodec.GetColumnTypeCodeInfo(type, out typeInfo);
                rs.Columns[i] =
                    new CqlColumn { Name = columnNames[i], TypeCode = typeCode, TypeInfo = typeInfo, Type = typeof(T), Index = i };
            }
            var columnIndexes = rs.Columns.ToDictionary(c => c.Name, c => c.Index);
            for (var i = 0; i < rowLength; i++)
            {
                var values = genericValues
                    .Select(v => (object)v)
                    .ToArray();
                var row = new Row(values, rs.Columns, columnIndexes);
                rs.AddRow(row);   
            }
            return rs;
        }

        public static RowSet GetUsersRowSet(IEnumerable<PlainUser> users)
        {
            var rs = new RowSet();
            rs.Columns = new[]
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
                var values = new object[]
                {
                    user.UserId,
                    user.Name,
                    user.Age,
                    user.CreatedDate,
                    user.IsActive,
                    user.LastLoginDate,
                    user.LoginHistory.ToArray(),
                    user.LuckyNumbers.ToArray(),
                    user.ChildrenAges,
                    (int)user.FavoriteColor,
                    (int?)user.TypeOfUser,
                    (int)user.PreferredContactMethod,
                    (int?)user.HairColor
                };
                var row = new Row(values, rs.Columns, columnIndexes);
                rs.AddRow(row);
            }
            return rs;
        }
    }
}
