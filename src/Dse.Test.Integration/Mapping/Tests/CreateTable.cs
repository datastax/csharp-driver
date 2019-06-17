//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Mapping.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using NUnit.Framework;

namespace Dse.Test.Integration.Mapping.Tests
{
    [Category("short")]
    public class CreateTable : SharedClusterTest
    {
        ISession _session;
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
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void CreateTable_FluentMapping_Success()
        {
            var mappingConfig = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(_session, mappingConfig);
            table.Create();

            var mapper = new Mapper(_session, mappingConfig);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);
            var cqlSelect = $"SELECT * from \"{table.Name}\" where \"StringType\"='{manyTypesInstance.StringType}'";
            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            instancesQueried[0].AssertEquals(manyTypesInstance);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_PartitionKeyOmitted()
        {
            var mappingWithoutPk = new Map<ManyDataTypesPoco>();
            var table = new Table<ManyDataTypesPoco>(_session, new MappingConfiguration().Define(mappingWithoutPk));

            var e = Assert.Throws<InvalidOperationException>(() => table.Create());
            var expectedErrMsg = "Cannot create CREATE statement for POCO of type " + typeof(ManyDataTypesPoco).Name + 
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
                .TableName("tbl_case_sens_once")
                .CaseSensitive());

            var table = new Table<ManyDataTypesPoco>(_session, config);
            table.Create();

            var mapper = new Mapper(_session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);
            var cqlSelect = $"SELECT * from \"{table.Name}\" where \"StringType\"='{manyTypesInstance.StringType}'";
            var objectsRetrieved = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, objectsRetrieved.Count);
            objectsRetrieved[0].AssertEquals(manyTypesInstance);
        }
    }
}
