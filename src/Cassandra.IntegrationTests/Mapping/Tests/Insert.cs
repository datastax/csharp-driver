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
using Cassandra.IntegrationTests.Linq.LinqMethods;
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
            IndividualTestSetup();
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
        /// Successfully insert a new record into a table that was created with fluent mapping, using Mapper.Insert
        /// </summary>
        [Test]
        public void Insert_WithMapperInsert()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepartitionkeylowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepartitionkeylowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Mapper.Insert
            lowercaseclassnamepartitionkeylowercase privateClassInstance = new lowercaseclassnamepartitionkeylowercase();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepartitionkeylowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepartitionkeylowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeylowercase defaultInstance = new lowercaseclassnamepartitionkeylowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, 
        /// using Session.Execute to insert an Insert object created with table.Insert()
        /// </summary>
        [Test]
        public void Insert_WithSessionExecuteTableInsert()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName();
            Assert.AreNotEqual(uniqueTableName.ToLower(), uniqueTableName);
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepartitionkeylowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepartitionkeylowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Session.Execute
            lowercaseclassnamepartitionkeylowercase defaultPocoInstance = new lowercaseclassnamepartitionkeylowercase();
            _session.Execute(table.Insert(defaultPocoInstance));
            var mapper = new Mapper(_session, mappingConfig);
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
        public void Insert_ClassAndPartitionKeyAreCamelCase()
        {
            var mappingConfig = new MappingConfiguration().Define(new Map<PrivateClassWithClassNameCamelCase>().PartitionKey(c => c.SomePartitionKey));
            Table<PrivateClassWithClassNameCamelCase> table = new Table<PrivateClassWithClassNameCamelCase>(_session, mappingConfig);
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration());
            PrivateClassWithClassNameCamelCase privateClassCamelCase = new PrivateClassWithClassNameCamelCase();
            mapper.Insert(privateClassCamelCase);

            List<lowercaseclassnamepartitionkeylowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepartitionkeylowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeylowercase defaultInstance = new lowercaseclassnamepartitionkeylowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);

            Assert.IsFalse(TestUtils.TableExists(_session, _uniqueKsName, typeof (PrivateClassWithClassNameCamelCase).Name));
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, typeof(PrivateClassWithClassNameCamelCase).Name.ToLower()));
        }

        /// <summary>
        /// Validate that mapped class properties are lower-cased by default
        /// </summary>
        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyCamelCase()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepartitionkeycamelcase>().PartitionKey(c => c.SomePartitionKey));
            Table<lowercaseclassnamepartitionkeycamelcase> table = new Table<lowercaseclassnamepartitionkeycamelcase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();
            var mapper = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepartitionkeycamelcase privateClassInstance = new lowercaseclassnamepartitionkeycamelcase();

            // Validate state of table
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepartitionkeycamelcase> instancesQueried = mapper.Fetch<lowercaseclassnamepartitionkeycamelcase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeycamelcase defaultPocoInstance = new lowercaseclassnamepartitionkeycamelcase();
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, instancesQueried[0].SomePartitionKey);

            // Attempt to select from Camel Case partition key
            string cqlCamelCasePartitionKey = "SELECT * from " + typeof (lowercaseclassnamepartitionkeycamelcase).Name + " where \"SomePartitionKey\" = 'doesntmatter'";
            var ex = Assert.Throws<InvalidQueryException>(() => _session.Execute(cqlCamelCasePartitionKey));
            string expectedErrMsg = "Undefined name SomePartitionKey in where clause";
            StringAssert.Contains(expectedErrMsg, ex.Message);

            // Validate that select on lower case key does not fail
            string cqlLowerCasePartitionKey = "SELECT * from " + typeof(lowercaseclassnamepartitionkeycamelcase).Name + " where \"somepartitionkey\" = '" + defaultPocoInstance.SomePartitionKey + "'";
            List<Row> rows = _session.Execute(cqlLowerCasePartitionKey).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
        }

        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyLowerCase()
        {
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepartitionkeylowercase>().PartitionKey(c => c.somepartitionkey));
            Table<lowercaseclassnamepartitionkeylowercase> table = new Table<lowercaseclassnamepartitionkeylowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepartitionkeylowercase defaultPocoInstance = new lowercaseclassnamepartitionkeylowercase();

            cqlClient.Insert(defaultPocoInstance);
            List<lowercaseclassnamepartitionkeylowercase> instancesQueried = cqlClient.Fetch<lowercaseclassnamepartitionkeylowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepartitionkeylowercase defaultInstance = new lowercaseclassnamepartitionkeylowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);

            // using standard cql
            string cqlLowerCasePartitionKey = "SELECT * from " + typeof(lowercaseclassnamepartitionkeylowercase).Name + " where \"somepartitionkey\" = '" + defaultPocoInstance.somepartitionkey + "'";
            List<Row> rows = _session.Execute(cqlLowerCasePartitionKey).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(defaultPocoInstance.somepartitionkey, rows[0].GetValue<string>("somepartitionkey"));

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
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepartitionkeycamelcase
        {
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepartitionkeylowercase
        {
            public string somepartitionkey = "somePartitionKey";
        }

        private class PocoWithAdditionalField
        {
            public string SomeString = "someStringValue";
            public string SomeOtherString = "someOtherStringValue";
        }


    }
}
