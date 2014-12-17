//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Tests;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Insert : TestGlobals
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
        public void Insert_IntoTableCreatedWithFluentMapping()
        {
            var config = new MappingConfiguration().Define(
                new Map<lowercaseclassnamepartitionkeylowercase>()
                .TableName("lowercaseclassnamepartitionkeylowercase")
                .PartitionKey(u => u.somepartitionkey));
            var table = new Table<lowercaseclassnamepartitionkeycamelcase>(_session, config);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepartitionkeylowercase privateClassInstance = new lowercaseclassnamepartitionkeylowercase();

            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepartitionkeylowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepartitionkeylowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeylowercase defaultInstance = new lowercaseclassnamepartitionkeylowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Attempt to insert a Poco into a nonexistent table
        /// </summary>
        [Test]
        public void Insert_UnconfiguredTable()
        {
            // Setup
            var mapper = new Mapper(_session, new MappingConfiguration());
            ManyDataTypesPoco manyTypesPoco = ManyDataTypesPoco.GetRandomInstance();

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => mapper.Insert(manyTypesPoco));
            string expectedErrMsg = "unconfigured columnfamily " + typeof(ManyDataTypesPoco).Name.ToLower();
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// By default Linq preserves class param casing, but cqlpoco does not, 
        /// so expect "unconfigured columnfamily" when trying to insert via cqlpoco using default settings
        /// This also validates that a private class can be used by the CqlPoco client
        /// </summary>
        [Test]
        public void Insert_TableCreatedWithLinq_MappedTableNameDefaultsToLowerCase()
        {
            var table = new Table<PrivateClassWithClassNameCamelCase>(_session, new MappingConfiguration());
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration());
            PrivateClassWithClassNameCamelCase privateClassCamelCase = new PrivateClassWithClassNameCamelCase();

            var e = Assert.Throws<InvalidQueryException>(() => mapper.Insert(privateClassCamelCase));
            string expectedErrMsg = "unconfigured columnfamily " + typeof(PrivateClassWithClassNameCamelCase).Name.ToLower();
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Validate that mapped class properties are lower-cased by default
        /// </summary>
        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyCamelCase()
        {
            // Setup
            var table = new Table<lowercaseclassnamepartitionkeycamelcase>(_session, new MappingConfiguration());
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();
            var mapper = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepartitionkeycamelcase privateClassInstance = new lowercaseclassnamepartitionkeycamelcase();

            // Validate Error Msg
            var ex = Assert.Throws<InvalidQueryException>(() => mapper.Insert(privateClassInstance));
            string expectedErrMsg = "Unknown identifier somepartitionkey";
            Assert.AreEqual(expectedErrMsg, ex.Message);
        }

        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyLowerCase()
        {
            var table = new Table<lowercaseclassnamepartitionkeycamelcase>(_session, new MappingConfiguration());
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepartitionkeylowercase privateClassInstance = new lowercaseclassnamepartitionkeylowercase();

            cqlClient.Insert(privateClassInstance);
            List<lowercaseclassnamepartitionkeylowercase> instancesQueried = cqlClient.Fetch<lowercaseclassnamepartitionkeylowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeylowercase defaultInstance = new lowercaseclassnamepartitionkeylowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Attempting to insert a Poco into a table with a missing column field fails
        /// </summary>
        [Test]
        public void Insert_MislabledClusteringKey()
        {
            string tableName = typeof(PocoWithAdditionalField).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY)";
            _session.Execute(createTableCql);
            var cqlClient = new Mapper(_session, new MappingConfiguration());
            PocoWithAdditionalField pocoWithCustomAttributes = new PocoWithAdditionalField();

            // Validate expected exception
            var ex = Assert.Throws<InvalidQueryException>(() => cqlClient.Insert(pocoWithCustomAttributes));
            StringAssert.Contains("Unknown identifier someotherstring", ex.Message);
        }


        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        private class PrivateClassWithClassNameCamelCase
        {
            [PartitionKey]
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepartitionkeycamelcase
        {
            [PartitionKey]
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepartitionkeylowercase
        {
            [PartitionKey]
            public string somepartitionkey = "somePartitionKey";
        }

        private class PocoWithAdditionalField
        {
            [PartitionKey]
            public string SomeString = "someStringValue";
            public string SomeOtherString = "someOtherStringValue";
        }


    }
}
