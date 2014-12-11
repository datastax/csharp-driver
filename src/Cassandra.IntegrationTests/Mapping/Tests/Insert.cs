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
using Cassandra.Tests.Mapping.FluentMappings;
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

        [Test]
        public void Insert_UnconfiguredTable()
        {
            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            ManyDataTypesPoco manyTypesPoco = ManyDataTypesPoco.GetRandomInstance();

            try
            {
                cqlClient.Insert(manyTypesPoco);
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "unconfigured columnfamily " + typeof(ManyDataTypesPoco).Name.ToLower();
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        /// <summary>
        /// By default Linq preserves class param casing, but cqlpoco does not, 
        /// so expect "unconfigured columnfamily" when trying to insert via cqlpoco using default settings
        /// This also validates that a private class can be used by the CqlPoco client
        /// </summary>
        [Test]
        public void Insert_TableNameDefaultsToLowerCase()
        {
            Table<PrivateClassWithClassNameCamelCase> table = _session.GetTable<PrivateClassWithClassNameCamelCase>();
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            PrivateClassWithClassNameCamelCase privateClassCamelCase = new PrivateClassWithClassNameCamelCase();

            try
            {
                cqlClient.Insert(privateClassCamelCase);
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "unconfigured columnfamily " + typeof(PrivateClassWithClassNameCamelCase).Name.ToLower();
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyCamelCase()
        {
            Table<lowercaseclassnamepartitionkeycamelcase> table = _session.GetTable<lowercaseclassnamepartitionkeycamelcase>();
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            lowercaseclassnamepartitionkeycamelcase privateClassInstance = new lowercaseclassnamepartitionkeycamelcase();

            try
            {
                cqlClient.Insert(privateClassInstance);
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "Unknown identifier somepartitionkey";
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyLowerCase()
        {
            Table<lowercaseclassnamepartitionkeylowercase> table = _session.GetTable<lowercaseclassnamepartitionkeylowercase>();
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
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
        public void Attributes_MislabledClusteringKey()
        {
            string tableName = typeof(PocoWithAdditionalField).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY)";
            _session.Execute(createTableCql);
            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
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
