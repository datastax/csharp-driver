//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    /// <summary>
    /// Use predefined classes that contain fluent mapping to manage Linq-mapped resources
    /// </summary>
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class FluentMappingPredefined : SharedClusterTest
    {
        [SetUp]
        public void SetupTest()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            Session.CreateKeyspace(keyspaceName);
            Session.ChangeKeyspace(keyspaceName);
        }

        /// <summary>
        /// Validate Fluent Mapping default case sensitivity rules
        /// </summary>
        [Test]
        public void Attributes_FluentMapping_CaseSensitive()
        {
            var config = new MappingConfiguration().Define(new ClassWithCamelCaseNameMapping());
            var table = new Table<ClassWithCamelCaseName>(Session, config);
            table.Create();

            var cqlClient = new Mapper(Session, config);
            ClassWithCamelCaseName classWithCamelCaseName = new ClassWithCamelCaseName
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(classWithCamelCaseName);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            string cqlSelect = "SELECT * from \"" + typeof(ClassWithCamelCaseName).Name + "\"";
            List<ClassWithCamelCaseName> records = cqlClient.Fetch<ClassWithCamelCaseName>(cqlSelect).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, records[0].SomePartitionKey);
            ClassWithCamelCaseName defaultInstance = new ClassWithCamelCaseName();
            Assert.AreEqual(defaultInstance.IgnoredStringAttribute, records[0].IgnoredStringAttribute);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, rows[0].GetValue<string>("SomePartitionKey"));
            var ex = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>("IgnoredStringAttribute"));
            StringAssert.Contains("Column IgnoredStringAttribute not found", ex.Message);
        }

        /// <summary>
        /// Validate that Fluent Mapping with CaseSensitive() method used on everything but the class name
        /// which is passed in a camel case string
        /// This should work the same as specifying CaseSensitive for the Class name
        /// </summary>
        [Test]
        public void Attributes_FluentMapping_CaseSensitive_NotSpecifiedForClassName()
        {
            var config = new MappingConfiguration().Define(new ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName());
            var table = new Table<ClassWithCamelCaseName>(Session, config);
            table.Create();

            var cqlClient = new Mapper(Session, config);
            ClassWithCamelCaseName classWithCamelCaseName = new ClassWithCamelCaseName
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(classWithCamelCaseName);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            string cqlSelect = "SELECT * from \"" + typeof(ClassWithCamelCaseName).Name + "\"";
            List<ClassWithCamelCaseName> records = cqlClient.Fetch<ClassWithCamelCaseName>(cqlSelect).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, records[0].SomePartitionKey);
            ClassWithCamelCaseName defaultInstance = new ClassWithCamelCaseName();
            Assert.AreEqual(defaultInstance.IgnoredStringAttribute, records[0].IgnoredStringAttribute);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, rows[0].GetValue<string>("SomePartitionKey"));
            var ex = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>("IgnoredStringAttribute"));
            StringAssert.Contains("Column IgnoredStringAttribute not found", ex.Message);
        }


        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        private class ClassWithCamelCaseName
        {
            public string SomePartitionKey = "somePartitionKeyDefaultValue";
            public string IgnoredStringAttribute = "someIgnoredStringDefaultValue";
        }

        class ClassWithCamelCaseNameMapping : Map<ClassWithCamelCaseName>
        {
            public ClassWithCamelCaseNameMapping()
            {
                TableName(typeof(ClassWithCamelCaseName).Name).CaseSensitive();
                PartitionKey(u => u.SomePartitionKey);
                Column(u => u.SomePartitionKey).CaseSensitive();
                Column(u => u.IgnoredStringAttribute, cm => cm.Ignore());
            }
        }

        class ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName : Map<ClassWithCamelCaseName>
        {
            public ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName()
            {
                TableName(typeof(ClassWithCamelCaseName).Name); // no case sensitivity mentioned
                PartitionKey(u => u.SomePartitionKey);
                Column(u => u.SomePartitionKey).CaseSensitive();
                Column(u => u.IgnoredStringAttribute, cm => cm.Ignore());
            }
        }



    }
}
