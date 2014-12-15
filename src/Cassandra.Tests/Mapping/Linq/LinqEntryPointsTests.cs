using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            Assert.AreEqual(@"SELECT * FROM ""AllTypesEntity""", table.ToString());
        }

        [Test]
        public void Deprecated_EntryPoint_Honors_Mapping_Defined()
        {
            MappingConfiguration.Global.Define(new Map<AllTypesEntity>().TableName("tbl1"));
            var table = SessionExtensions.GetTable<AllTypesEntity>(null);
            Assert.AreEqual(@"SELECT * FROM tbl1", table.ToString());
        }

        [Test]
        public void Table_Constructor_Defaults_To_MappingAttributesAttributes()
        {
            var table = new Table<AllTypesEntity>(null);
            Assert.AreEqual(@"SELECT * FROM AllTypesEntity", table.ToString());
        }

        [Test]
        public void Table_Constructor_Uses_Provided_Mappings()
        {
            var table = new Table<AllTypesEntity>(null);
            Assert.AreEqual(@"SELECT * FROM AllTypesEntity", table.ToString());
            var config = new MappingConfiguration().Define(new Map<AllTypesEntity>().TableName("tbl3"));
            table = new Table<AllTypesEntity>(null, config);
            Assert.AreEqual(@"SELECT * FROM tbl3", table.ToString());
        }
    }
}
