//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqEntryPointsTests : MappingTestBase
    {
        [Test]
        public void Deprecated_EntryPoint_Defaults_To_LinqBasedAttributes()
        {
            var table = SessionExtensions.GetTable<AllTypesEntity>(null);
            Assert.AreEqual(
                @"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""Int64Value"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""AllTypesEntity""",
                table.ToString());
        }

        [Test]
        public void Deprecated_EntryPoint_Honors_Mapping_Defined()
        {
            MappingConfiguration.Global.Define(new Map<AllTypesEntity>().TableName("tbl1"));
            var table = SessionExtensions.GetTable<AllTypesEntity>(null);
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl1",
                table.ToString());
        }

        [Test]
        public void Deprecated_EntryPoint_Uses_Table_Provided()
        {
            MappingConfiguration.Global.Define(new Map<AllTypesEntity>().TableName("tbl1"));
            var table = SessionExtensions.GetTable<AllTypesEntity>(null, "linqTable");
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM linqTable",
                table.ToString());
        }

        [Test]
        public void Deprecated_EntryPoint_Uses_Keyspace_Provided()
        {
            MappingConfiguration.Global.Define(new Map<AllTypesEntity>().TableName("tbl1"));
            var table = SessionExtensions.GetTable<AllTypesEntity>(null, "linqTable", "linqKs");
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM linqKs.linqTable",
                table.ToString());
        }

        [Test]
        public void Table_Constructor_Defaults_To_MappingAttributesAttributes()
        {
            var table = new Table<AllTypesEntity>(null);
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM AllTypesEntity",
                table.ToString());
        }

        [Test]
        public void Table_Constructor_Uses_Provided_Mappings()
        {
            var table = new Table<AllTypesEntity>(null);
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM AllTypesEntity",
                table.ToString());
            var config = new MappingConfiguration().Define(new Map<AllTypesEntity>().TableName("tbl3"));
            table = new Table<AllTypesEntity>(null, config);
            Assert.AreEqual(@"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl3",
                table.ToString());
        }

        [Test]
        public void Table_Constructor_Uses_Provided_Mappings_With_Custom_TableName()
        {
            var config = new MappingConfiguration().Define(new Map<AllTypesEntity>().TableName("tbl4").Column(t => t.Int64Value, cm => cm.WithName("id1")));
            var table = new Table<AllTypesEntity>(null, config, "tbl_overridden1");
            Assert.AreEqual(
                @"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, id1, IntValue, StringValue, UuidValue FROM tbl_overridden1 WHERE id1 = ?",
                table.Where(t => t.Int64Value == 1).ToString());
        }

        [Test]
        public void Table_Constructor_Uses_Provided_Mappings_With_Custom_Keyspace()
        {
            var config = new MappingConfiguration().Define(new Map<AllTypesEntity>().TableName("tbl4").Column(t => t.Int64Value, cm => cm.WithName("id1")));
            var table = new Table<AllTypesEntity>(null, config, null, "ks_overridden1");
            Assert.AreEqual(@"SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, id1, IntValue, StringValue, UuidValue FROM ks_overridden1.tbl4 WHERE id1 = ?", table.Where(t => t.Int64Value == 1).ToString());
        }

        [Test]
        public void Table_Constructor_Uses_Provided_Mappings_With_Custom_Keyspace_And_TableName()
        {
            var config = new MappingConfiguration().Define(new Map<AllTypesEntity>().TableName("tbl4").CaseSensitive().Column(t => t.Int64Value, cm => cm.WithName("id1")));
            var table = new Table<AllTypesEntity>(null, config, "tbl_custom", "ks_custom");
            Assert.AreEqual(
                @"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""id1"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""ks_custom"".""tbl_custom"" WHERE ""id1"" = ?",
                table.Where(t => t.Int64Value == 1).ToString());
        }
    }
}
