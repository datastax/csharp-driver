using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.FluentMappings;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class FluentMapping : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(Attributes));
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
        /// Validate that Fluent Mapping 
        /// </summary>
        [Test]
        public void Attributes_FluentMapping_CaseSensitive()
        {
            Table<ClassWithCamelCaseName> table = _session.GetTable<ClassWithCamelCaseName>();
            table.Create();

            var cqlClient =
                CqlClientConfiguration.
                ForSession(_session).
                UseIndividualMapping<ClassWithCamelCaseNameMapping>().
                BuildCqlClient();
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
            List<Row> rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, rows[0].GetValue<string>("SomePartitionKey"));
            Assert.AreEqual(null, rows[0].GetValue<string>("IgnoredStringAttribute"));
        }

        /// <summary>
        /// Validate that Fluent Mapping with CaseSensitive() method used on everything but the class name
        /// which is passed in a camel case string
        /// This should work the same as specifying CaseSensitive for the Class name
        /// </summary>
        [Test]
        public void Attributes_FluentMapping_CaseSensitive_NotSpecifiedForClassName()
        {
            Table<ClassWithCamelCaseName> table = _session.GetTable<ClassWithCamelCaseName>();
            table.Create();

            var cqlClient =
                CqlClientConfiguration.
                ForSession(_session).
                UseIndividualMapping<ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName>().
                BuildCqlClient();
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
            List<Row> rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(classWithCamelCaseName.SomePartitionKey, rows[0].GetValue<string>("SomePartitionKey"));
            Assert.AreEqual(null, rows[0].GetValue<string>("IgnoredStringAttribute"));
        }


        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        private class ClassWithCamelCaseName
        {
            [Cassandra.Data.Linq.PartitionKey]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            public string IgnoredStringAttribute = "someIgnoredStringDefaultValue";
        }

        class ClassWithCamelCaseNameMapping : Map<ClassWithCamelCaseName>
        {
            public ClassWithCamelCaseNameMapping()
            {
                TableName(typeof(ClassWithCamelCaseName).Name).CaseSensitive();
                Column(u => u.SomePartitionKey).CaseSensitive();
                Column(u => u.IgnoredStringAttribute, cm => cm.Ignore());
            }
        }

        class ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName : Map<ClassWithCamelCaseName>
        {
            public ClassWithCamelCaseNameMapping_CaseSensitiveNotSpecifiedForClassName()
            {
                TableName(typeof(ClassWithCamelCaseName).Name); // no case sensitivity mentioned
                Column(u => u.SomePartitionKey).CaseSensitive();
                Column(u => u.IgnoredStringAttribute, cm => cm.Ignore());
            }
        }



    }
}
