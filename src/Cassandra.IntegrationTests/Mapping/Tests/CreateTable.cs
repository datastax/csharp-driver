using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Tests;
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
            _session = TestClusterManager.GetTestCluster(1).Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void CreateTable_FluentMapping_Success()
        {
            var table = _session.GetTable<ManyDataTypesPoco>(new ManyDataTypesPocoMappingCaseSensitive());
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping <ManyDataTypesPocoMappingCaseSensitive>()
                .BuildCqlClient();
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            cqlClient.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            List<ManyDataTypesPoco> instancesQueried = cqlClient.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            instancesQueried[0].AssertEquals(manyTypesInstance);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_PartitionKeyOmitted()
        {
            Map<ManyDataTypesPoco> mappingWithoutPk = new Map<ManyDataTypesPoco>() {};
            var table = _session.GetTable<ManyDataTypesPoco>(mappingWithoutPk);

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
            Map<ManyDataTypesPoco> mappingInstance = new Map<ManyDataTypesPoco>();
            mappingInstance.PartitionKey(u => u.StringType);
            mappingInstance.CaseSensitive();

            var table = _session.GetTable<ManyDataTypesPoco>(mappingInstance);
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping <ManyDataTypesPocoMappingCaseSensitive>()
                .BuildCqlClient();
            ManyDataTypesPoco manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            cqlClient.Insert(manyTypesInstance);
            string cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);
            List<ManyDataTypesPoco> objectsRetrieved = cqlClient.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, objectsRetrieved.Count);
            objectsRetrieved[0].AssertEquals(manyTypesInstance);


        }



    }
}
