using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class DataTypeParserTests
    {
        [Test]
        public void ParseDataTypeNameSingleTest()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.Int32Type");
            Assert.AreEqual(ColumnTypeCode.Int, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.UUIDType");
            Assert.AreEqual(ColumnTypeCode.Uuid, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.UTF8Type");
            Assert.AreEqual(ColumnTypeCode.Varchar, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.BytesType");
            Assert.AreEqual(ColumnTypeCode.Blob, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.FloatType");
            Assert.AreEqual(ColumnTypeCode.Float, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DoubleType");
            Assert.AreEqual(ColumnTypeCode.Double, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.BooleanType");
            Assert.AreEqual(ColumnTypeCode.Boolean, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.InetAddressType");
            Assert.AreEqual(ColumnTypeCode.Inet, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DateType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimestampType");
            Assert.AreEqual(ColumnTypeCode.Timestamp, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.LongType");
            Assert.AreEqual(ColumnTypeCode.Bigint, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DecimalType");
            Assert.AreEqual(ColumnTypeCode.Decimal, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.IntegerType");
            Assert.AreEqual(ColumnTypeCode.Varint, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.CounterColumnType");
            Assert.AreEqual(ColumnTypeCode.Counter, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimeUUIDType");
            Assert.AreEqual(ColumnTypeCode.Timeuuid, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.AsciiType");
            Assert.AreEqual(ColumnTypeCode.Ascii, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SimpleDateType");
            Assert.AreEqual(ColumnTypeCode.Date, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimeType");
            Assert.AreEqual(ColumnTypeCode.Time, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ShortType");
            Assert.AreEqual(ColumnTypeCode.SmallInt, dataType.TypeCode);
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ByteType");
            Assert.AreEqual(ColumnTypeCode.TinyInt, dataType.TypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Multiple_Test()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, ((ListColumnInfo) dataType.TypeInfo).ValueTypeCode);

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Uuid, ((SetColumnInfo) dataType.TypeInfo).KeyTypeCode);

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TimeUUIDType)");
            Assert.AreEqual(ColumnTypeCode.Set, dataType.TypeCode);
            Assert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Timeuuid, ((SetColumnInfo) dataType.TypeInfo).KeyTypeCode);

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)");
            Assert.AreEqual(ColumnTypeCode.Map, dataType.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Varchar, ((MapColumnInfo) dataType.TypeInfo).KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.Bigint, ((MapColumnInfo) dataType.TypeInfo).ValueTypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Frozen_Test()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TimeUUIDType))");
            Assert.AreEqual(ColumnTypeCode.List, dataType.TypeCode);
            Assert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Timeuuid, ((ListColumnInfo) dataType.TypeInfo).ValueTypeCode);

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))");
            Assert.AreEqual(ColumnTypeCode.Map, dataType.TypeCode);
            Assert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual(ColumnTypeCode.Varchar, ((MapColumnInfo) dataType.TypeInfo).KeyTypeCode);
            Assert.AreEqual(ColumnTypeCode.List, ((MapColumnInfo) dataType.TypeInfo).ValueTypeCode);
            var subType = (ListColumnInfo)(((MapColumnInfo) dataType.TypeInfo).ValueTypeInfo);
            Assert.AreEqual(ColumnTypeCode.Int, subType.ValueTypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Udt_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester,70686f6e65,616c696173:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type" +
                ")";
            var dataType = DataTypeParser.ParseFqTypeName(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            //Udt name
            Assert.AreEqual("phone", dataType.Name);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            var subTypes = ((UdtColumnInfo) dataType.TypeInfo).Fields;
            Assert.AreEqual(2, subTypes.Count);
            Assert.AreEqual("alias", subTypes[0].Name);
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[0].TypeCode);
            Assert.AreEqual("number", subTypes[1].Name);
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[1].TypeCode);
        }

        [Test]
        public void Parse_DataType_Name_Udt_Nested_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester," +
                    "61646472657373," +
                    "737472656574:org.apache.cassandra.db.marshal.UTF8Type," +
                    "5a4950:org.apache.cassandra.db.marshal.Int32Type," +
                    "70686f6e6573:org.apache.cassandra.db.marshal.SetType(" +
                    "org.apache.cassandra.db.marshal.UserType(" +
                        "tester," +
                        "70686f6e65," +
                        "616c696173:org.apache.cassandra.db.marshal.UTF8Type," +
                        "6e756d626572:org.apache.cassandra.db.marshal.UTF8Type))" +
                ")";
            var dataType = DataTypeParser.ParseFqTypeName(typeText);
            Assert.AreEqual(ColumnTypeCode.Udt, dataType.TypeCode);
            Assert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            Assert.AreEqual("address", dataType.Name);
            Assert.AreEqual("tester.address", ((UdtColumnInfo) dataType.TypeInfo).Name);
            var subTypes = ((UdtColumnInfo) dataType.TypeInfo).Fields;
            Assert.AreEqual(3, subTypes.Count);
            Assert.AreEqual("street,ZIP,phones", String.Join(",", subTypes.Select(s => s.Name)));
            Assert.AreEqual(ColumnTypeCode.Varchar, subTypes[0].TypeCode);
            Assert.AreEqual(ColumnTypeCode.Set, subTypes[2].TypeCode);
            //field name
            Assert.AreEqual("phones", subTypes[2].Name);

            var phonesSubType = (UdtColumnInfo)((SetColumnInfo)subTypes[2].TypeInfo).KeyTypeInfo;
            Assert.AreEqual("tester.phone", phonesSubType.Name);
            Assert.AreEqual(2, phonesSubType.Fields.Count);
            Assert.AreEqual("alias", phonesSubType.Fields[0].Name);
            Assert.AreEqual("number", phonesSubType.Fields[1].Name);
        }

        [Test]
        public void ParseTypeName_Should_Parse_Single_Cql_Types()
        {
            var cqlNames = new Dictionary<string, ColumnTypeCode>
            {
                {"varchar", ColumnTypeCode.Varchar},
                {"text", ColumnTypeCode.Text},
                {"ascii", ColumnTypeCode.Ascii},
                {"uuid", ColumnTypeCode.Uuid},
                {"timeuuid", ColumnTypeCode.Timeuuid},
                {"int", ColumnTypeCode.Int},
                {"blob", ColumnTypeCode.Blob},
                {"float", ColumnTypeCode.Float},
                {"double", ColumnTypeCode.Double},
                {"boolean", ColumnTypeCode.Boolean},
                {"inet", ColumnTypeCode.Inet},
                {"date", ColumnTypeCode.Date},
                {"time", ColumnTypeCode.Time},
                {"smallint", ColumnTypeCode.SmallInt},
                {"tinyint", ColumnTypeCode.TinyInt},
                {"timestamp", ColumnTypeCode.Timestamp},
                {"bigint", ColumnTypeCode.Bigint},
                {"decimal", ColumnTypeCode.Decimal},
                {"varint", ColumnTypeCode.Varint},
                {"counter", ColumnTypeCode.Counter}
            };
            foreach (var kv in cqlNames)
            {
                var type = DataTypeParser.ParseTypeName(null, null, kv.Key).Result;
                Assert.NotNull(type);
                Assert.AreEqual(kv.Value, type.TypeCode);
                Assert.Null(type.TypeInfo);
            }
        }

        [Test]
        public void ParseTypeName_Should_Parse_Frozen_Cql_Types()
        {
            var cqlNames = new Dictionary<string, ColumnTypeCode>
            {
                {"frozen<varchar>", ColumnTypeCode.Varchar},
                {"frozen<list<int>>", ColumnTypeCode.List},
                {"frozen<map<text,frozen<list<int>>>>", ColumnTypeCode.Map}
            };
            foreach (var kv in cqlNames)
            {
                var type = DataTypeParser.ParseTypeName(null, null, kv.Key).Result;
                Assert.NotNull(type);
                Assert.AreEqual(kv.Value, type.TypeCode);
                Assert.AreEqual(true, type.IsFrozen);
            }
        }

        [Test]
        public void ParseTypeName_Should_Parse_Collections()
        {
            {
                var type = DataTypeParser.ParseTypeName(null, null, "list<int>").Result;
                Assert.NotNull(type);
                Assert.AreEqual(ColumnTypeCode.List, type.TypeCode);
                var subTypeInfo = (ListColumnInfo)type.TypeInfo;
                Assert.AreEqual(ColumnTypeCode.Int, subTypeInfo.ValueTypeCode);
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "set<uuid>").Result;
                Assert.NotNull(type);
                Assert.AreEqual(ColumnTypeCode.Set, type.TypeCode);
                var subTypeInfo = (SetColumnInfo)type.TypeInfo;
                Assert.AreEqual(ColumnTypeCode.Uuid, subTypeInfo.KeyTypeCode);
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "map<text, timeuuid>").Result;
                Assert.NotNull(type);
                Assert.AreEqual(ColumnTypeCode.Map, type.TypeCode);
                var subTypeInfo = (MapColumnInfo)type.TypeInfo;
                Assert.AreEqual(ColumnTypeCode.Text, subTypeInfo.KeyTypeCode);
                Assert.AreEqual(ColumnTypeCode.Timeuuid, subTypeInfo.ValueTypeCode);
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "map<text,frozen<list<int>>>").Result;
                Assert.NotNull(type);
                Assert.AreEqual(ColumnTypeCode.Map, type.TypeCode);
                var subTypeInfo = (MapColumnInfo)type.TypeInfo;
                Assert.AreEqual(ColumnTypeCode.Text, subTypeInfo.KeyTypeCode);
                Assert.AreEqual(ColumnTypeCode.List, subTypeInfo.ValueTypeCode);
                var subListTypeInfo = (ListColumnInfo)subTypeInfo.ValueTypeInfo;
                Assert.AreEqual(ColumnTypeCode.Int, subListTypeInfo.ValueTypeCode);
            }
        }

        [Test]
        public async Task ParseTypeName_Should_Parse_Custom_Types()
        {
            var typeNames = new[]
            {
              "org.apache.cassandra.db.marshal.MyCustomType",
              "com.datastax.dse.whatever.TypeName"
            };
            foreach (var typeName in typeNames)
            {
                var type = await DataTypeParser.ParseTypeName(null, null, string.Format("'{0}'", typeName)).ConfigureAwait(false);
                Assert.AreEqual(ColumnTypeCode.Custom, type.TypeCode);
                var info = (CustomColumnInfo)type.TypeInfo;
                Assert.AreEqual(typeName, info.CustomTypeName);
            }
        }

        [Test]
        public void ParseFqTypeName_Should_Parse_Custom_Types()
        {
            var typeNames = new[]
            {
              "org.apache.cassandra.db.marshal.MyCustomType",
              "com.datastax.dse.whatever.TypeName"
            };
            foreach (var typeName in typeNames)
            {
                var type = DataTypeParser.ParseFqTypeName(typeName);
                Assert.AreEqual(ColumnTypeCode.Custom, type.TypeCode);
                var info = (CustomColumnInfo)type.TypeInfo;
                Assert.AreEqual(typeName, info.CustomTypeName);
            }
        }
    }
}
