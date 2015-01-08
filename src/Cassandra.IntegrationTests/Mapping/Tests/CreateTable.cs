using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.LinqMethods;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class CreateTable : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(CreateTable));
        string _uniqueKsName;

        [SetUp]
        public void SetupTest()
        {
            IndividualTestSetup();
            _session = TestClusterManager.GetTestCluster(1).Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void CreateTable_FluentMapping_Success()
        {
            var mappingConfig = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, mappingConfig);
            table.Create();

            var mapper = new Mapper(_session, mappingConfig);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            List<ManyDataTypesPoco> instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            instancesQueried[0].AssertEquals(manyTypesInstance);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_PartitionKeyOmitted()
        {
            Map<ManyDataTypesPoco> mappingWithoutPk = new Map<ManyDataTypesPoco>();
            var table = new Table<ManyDataTypesPoco>(_session, new MappingConfiguration().Define(mappingWithoutPk));

            var e = Assert.Throws<InvalidOperationException>(() => table.Create());
            string expectedErrMsg = "Cannot create CREATE statement for POCO of type " + typeof(ManyDataTypesPoco).Name + 
                " because it is missing PK columns id.  Are you missing a property/field on the POCO or did you forget to specify the PK columns in the mapping?";
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_MakeAllPropertiesCaseSensitiveAtOnce()
        {
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(u => u.StringType)
                .CaseSensitive());

            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.Create();

            var mapper = new Mapper(_session, config);
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            List<ManyDataTypesPoco> objectsRetrieved = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, objectsRetrieved.Count);
            objectsRetrieved[0].AssertEquals(manyTypesInstance);
        }
    }
}
