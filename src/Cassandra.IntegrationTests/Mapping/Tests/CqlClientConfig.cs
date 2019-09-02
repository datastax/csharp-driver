using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short"), Category("realcluster")]
    public class CqlClientConfig : SharedClusterTest
    {
        private ISession _session;
        string _uniqueKsName;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object that was created with fluent mapping, 
        /// using a statically defined mapping class
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappingGeneric_StaticMappingClass()
        {
            // Use separate keyspace to avoid interfering with other tests
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, config);
            Assert.AreNotEqual(table.Name, table.Name.ToLower()); // make sure the case sensitivity rule is being used
            table.CreateIfNotExists();

            var mapper = new Mapper(_session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>().ToList();
            Assert.AreEqual(instancesQueried.Count, 1);
            manyTypesInstance.AssertEquals(instancesQueried[0]);
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object that was created with fluent mapping defined at run time,
        /// using UseIndividualMapping method that that uses general Poco type
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappingClassType_StaticMappingClass()
        {
            // Use separate keyspace to avoid interfering with other tests
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.CreateIfNotExists();

            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            ManyDataTypesPoco.KeepTryingSelectAndAssert(mapper, cqlSelect, new List<ManyDataTypesPoco>() { manyTypesInstance });
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object using the method UseIndividualMappings() 
        /// that uses a fluent mapping rule that was created during runtime
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappings_MappingDefinedDuringRuntime()
        {
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(c => c.StringType)
                .CaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.CreateIfNotExists();

            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            ManyDataTypesPoco.KeepTryingSelectAndAssert(mapper, cqlSelect, new List<ManyDataTypesPoco>() { manyTypesInstance });
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object using the method UseIndividualMappings() 
        /// that uses a fluent mapping rule derived from a static pre-defined mapping class
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappings_StaticMappingClass()
        {
            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.CreateIfNotExists();

            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            ManyDataTypesPoco.KeepTryingSelectAndAssert(mapper, cqlSelect, new List<ManyDataTypesPoco>() { manyTypesInstance });
        }

        /// <summary>
        /// Successfully insert a Poco instance 
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappings_EmptyTypeDefinitionList()
        {
            // Setup
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(c => c.StringType));
            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.CreateIfNotExists();

            // validate default lower-casing
            Assert.AreNotEqual(typeof(ManyDataTypesPoco).Name.ToLower(), typeof(ManyDataTypesPoco).Name);
            Assert.AreNotEqual(table.Name.ToLower(), table.Name);
            Assert.AreEqual(typeof(ManyDataTypesPoco).Name.ToLower(), table.Name.ToLower());

            // Test
            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);

            // Verify results
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name.ToLower(), "stringtype", manyTypesInstance.StringType);
            ManyDataTypesPoco.KeepTryingSelectAndAssert(mapper, cqlSelect, new List<ManyDataTypesPoco>() { manyTypesInstance });
        }

        /// <summary>
        /// Successfully insert a Poco instance withouth specifying any mapping type
        /// </summary>
        [Test]
        public void CqlClientConfiguration_MappingOmitted()
        {
            // Setup
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(c => c.StringType));
            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.Create();

            // Test
            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);

            // Check results
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name.ToLower(), "stringtype", manyTypesInstance.StringType);
            ManyDataTypesPoco.KeepTryingSelectAndAssert(mapper, cqlSelect, new List<ManyDataTypesPoco>() { manyTypesInstance });
        }


        /// <summary>
        /// 
        /// </summary>
        [Test, TestCassandraVersion(2,0)]
        public void CqlClientConfiguration_UseIndividualMapping_Default()
        {
            // Use separate keyspace to avoid interfering with other tests
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, config);
            Assert.AreNotEqual(table.Name, table.Name.ToLower()); // make sure the case sensitivity rule is being used
            table.CreateIfNotExists();

            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            List<ManyDataTypesPoco> instancesQueried = mapper.Fetch<ManyDataTypesPoco>().ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            manyTypesInstance.AssertEquals(instancesQueried[0]);
        }
    }
}
