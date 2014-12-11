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
    public class Attributes : TestGlobals
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
        /// Validate that the mapping mechanism ignores the class variable marked as "Ignore"
        /// The fact that the request does not fail trying to find a non-existing custom named column proves that 
        /// the request is not looking for the column for reads or writes.
        /// 
        /// This also validates that attributes from  Cassandra.Mapping and Cassandra.Data.Lync can be used successfully on the same object
        /// </summary>
        [Test]
        public void Attributes_Ignore()
        {
            Table<PocoWithIgnoredAttributes> table = _session.GetTable<PocoWithIgnoredAttributes>();
            table.Create();

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithIgnoredAttributes pocoWithCustomAttributes = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };

            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithIgnoredAttributes> records = cqlClient.Fetch<PocoWithIgnoredAttributes>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey, records[0].SomePartitionKey);
            PocoWithIgnoredAttributes defaultPocoWithIgnoredAttributes = new PocoWithIgnoredAttributes();
            Assert.AreEqual(defaultPocoWithIgnoredAttributes.IgnoredStringAttribute, records[0].IgnoredStringAttribute);
            Assert.AreEqual(defaultPocoWithIgnoredAttributes.SomeNonIgnoredDouble, records[0].SomeNonIgnoredDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
            Assert.AreEqual(pocoWithCustomAttributes.SomeNonIgnoredDouble, rows[0].GetValue<double>("somenonignoreddouble"));
            Assert.AreEqual(null, rows[0].GetValue<string>("this_should_be_ignored"));
        }

        /// <summary>
        /// Verify that when trying to insert a Poco with the partition key equal to null, it fails silently
        /// </summary>
        [Test]
        public void Attributes_PartitionKey_ValueNull()
        {
            Table<PocoWithIgnoredAttributes> table = _session.GetTable<PocoWithIgnoredAttributes>();
            table.Create();

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithIgnoredAttributes pocoWithCustomAttributes = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = null,
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object
            List<PocoWithIgnoredAttributes> records = cqlClient.Fetch<PocoWithIgnoredAttributes>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(0, records.Count);

            // Query for the column that the Linq table create created
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(0, rows.Count);
        }

        /// <summary>
        /// Validate that an insert query which includes a Poco that has the wrong field labeled as "PartitionKey" will silently fail.
        /// </summary>
        [Test]
        public void Attributes_PartitionKey_OnWrongField()
        {
            // Create table using Linq -- includes partition key that is not assigned by the mapping attribute
            Table<PocoWithWrongFieldLabeledPartitionKey> table = _session.GetTable<PocoWithWrongFieldLabeledPartitionKey>();
            table.Create();
            string cqlSelectAll = "SELECT * from " + table.Name;

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithWrongFieldLabeledPartitionKey pocoWithCustomAttributes = new PocoWithWrongFieldLabeledPartitionKey();
            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithWrongFieldLabeledPartitionKey> records = cqlClient.Fetch<PocoWithWrongFieldLabeledPartitionKey>(cqlSelectAll).ToList();
            Assert.AreEqual(0, records.Count);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute(cqlSelectAll).GetRows().ToList();
            Assert.AreEqual(0, rows.Count); // TODO: Question -- Insert succeeded but then subsequent fetch failed ?
        }


        /// <summary>
        /// Verify that inserting a mapped object that totally omits the Cassandra.Mapping.Attributes.PartitionKey silently fails.
        /// However, using mapping and a differnt Poco that has the key, records can be inserted and fetched into the same table
        /// </summary>
        [Test]
        public void Attributes_PartitionKeyAttributeOmitted_FixedWithMapping()
        {
            // Setup
            string tableName = typeof(PocoWithPartitionKeyOmitted).Name.ToLower();
            string selectAllCql = "SELECT * from " + tableName;
            List<String> stringList = new List<string>() { "string1", "string2" };
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, somelist list<varchar>, somedouble double)";
            _session.Execute(createTableCql);

            // Now re-instantiate the cqlClient, but with mapping rule that resolves the missing key issue
            var cqlClientWithMappping = CqlClientConfiguration.
                    ForSession(_session).
                    UseIndividualMapping<PocoWithPartitionKeyIncludedMapping>().
                    BuildCqlClient();

            // insert new record
            PocoWithPartitionKeyIncluded pocoWithCustomAttributesKeyIncluded = new PocoWithPartitionKeyIncluded();
            pocoWithCustomAttributesKeyIncluded.SomeList = stringList; // make it not empty
            cqlClientWithMappping.Insert(pocoWithCustomAttributesKeyIncluded);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithPartitionKeyIncluded> records_1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records_1.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, records_1[0].SomeString);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeList, records_1[0].SomeList);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, records_1[0].SomeDouble);
            records_1.Clear();

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute(selectAllCql).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, rows[0].GetValue<string>("somestring"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeList, rows[0].GetValue<List<string>>("somelist"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, rows[0].GetValue<double>("somedouble"));

            // try to Select new record using poco that does not contain partition key, validate that the mapping mechanism matches what it can
            var cqlClientNomapping = CqlClientConfiguration.
                ForSession(_session).
                BuildCqlClient();
            List<PocoWithPartitionKeyOmitted> records_2 = cqlClientNomapping.Fetch<PocoWithPartitionKeyOmitted>(selectAllCql).ToList();
            Assert.AreEqual(1, records_2.Count);
            records_2.Clear();

            // try again with the old CqlClient instance
            records_1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records_1.Count);

            // Clear out the table, verify
            string truncateCql = "TRUNCATE " + tableName;
            _session.Execute(truncateCql);
            records_1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(0, records_1.Count); // should have gone from 1 to 0 records

            // Try inserting Poco that does not include the partition key
            PocoWithPartitionKeyOmitted pocoWithCustomAttributesKeyOmitted = new PocoWithPartitionKeyOmitted();
            pocoWithCustomAttributesKeyOmitted.SomeList = stringList; // make it not empty
            cqlClientNomapping.Insert(pocoWithCustomAttributesKeyOmitted);
            records_2 = cqlClientNomapping.Fetch<PocoWithPartitionKeyOmitted>(selectAllCql).ToList();
            Assert.AreEqual(0, records_2.Count);

            // Validate no records were inserted with standard CQL query
            rows = _session.Execute(selectAllCql).GetRows().ToList();
            Assert.AreEqual(0, rows.Count); 

        }


        /// <summary>
        /// Verify that we can use a mapped object to insert / query into a table 
        /// if we leave out a column that is not the partition key (in this case the list column)
        /// </summary>
        [Test]
        public void Attributes_NonPartitionKeyFieldOmittedFromPocoClass()
        {
            string tableName = typeof(PocoWithPartitionKeyIncluded).Name.ToLower();
            string selectAllCql = "SELECT * from " + tableName;
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, somelist list<text>, somelist2 list<text>, somedouble double)";
            _session.Execute(createTableCql);
            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();

            // insert new record
            PocoWithPartitionKeyIncluded pocoWithCustomAttributesKeyIncluded = new PocoWithPartitionKeyIncluded();
            cqlClient.Insert(pocoWithCustomAttributesKeyIncluded);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithPartitionKeyIncluded> records = cqlClient.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, records[0].SomeString);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, records[0].SomeDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute(selectAllCql).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, rows[0].GetValue<string>("somestring"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, rows[0].GetValue<double>("somedouble"));
        }

        /// <summary>
        /// Verify that inserting a mapped object without specifying Cassandra.Mapping.Attributes.PartitionKey does not fail
        /// This also validates that not all columns need to be included for the Poco insert / fetch to succeed
        /// </summary>
        [Test]
        public void Attributes_PartitionKeyNotLabeled()
        {
            string tableName = typeof(PocoWithOnlyPartitionKeyNotLabeled).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, somelist list<text>, somedouble double)";
            _session.Execute(createTableCql);

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            List<String> stringList = new List<string>() { "string1", "string2" };
            PocoWithOnlyPartitionKeyNotLabeled pocoWithOnlyCustomAttributes = new PocoWithOnlyPartitionKeyNotLabeled();
            cqlClient.Insert(pocoWithOnlyCustomAttributes); // TODO: Question -- Should this fail?

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithOnlyPartitionKeyNotLabeled> records = cqlClient.Fetch<PocoWithOnlyPartitionKeyNotLabeled>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithOnlyCustomAttributes.SomeString, records[0].SomeString);
            PocoWithOnlyPartitionKeyNotLabeled defaultPocoWithOnlyIgnoredAttributes = new PocoWithOnlyPartitionKeyNotLabeled();

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute("SELECT * from " + tableName).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithOnlyCustomAttributes.SomeString, rows[0].GetValue<string>("somestring"));
        }

        /// <summary>
        /// Verify that inserting a mapped object without including PartitionKey succeeds when it is not the only field in the Poco class
        /// </summary>
        [Test]
        public void Attributes_PartitionKeyNotLabeled_AnotherNonLabelFieldIncluded()
        {
            string tableName = typeof(PocoWithPartitionKeyNotLabeledAndOtherField).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, someotherstring text, somelist list<text>, somedouble double)";
            _session.Execute(createTableCql);

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithPartitionKeyNotLabeledAndOtherField pocoWithOnlyCustomAttributes = new PocoWithPartitionKeyNotLabeledAndOtherField();
            cqlClient.Insert(pocoWithOnlyCustomAttributes); 

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithPartitionKeyNotLabeledAndOtherField> records = cqlClient.Fetch<PocoWithPartitionKeyNotLabeledAndOtherField>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
        }


        /// <summary>
        /// Verify that inserting a mapped object, mislabeling the PartitionKey as a Clustering Key does not fail
        /// </summary>
        [Test]
        public void Attributes_MislabledClusteringKey()
        {
            string tableName = typeof(PocoMislabeledClusteringKey).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring varchar PRIMARY KEY)";
            _session.Execute(createTableCql);

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoMislabeledClusteringKey pocoWithCustomAttributes = new PocoMislabeledClusteringKey();
            cqlClient.Insert(pocoWithCustomAttributes); // TODO: Should this fail?

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoMislabeledClusteringKey> records = cqlClient.Fetch<PocoMislabeledClusteringKey>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomeString, records[0].SomeString);

        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_CompositeKey()
        {
            Table<PocoWithCompositeKey> table = _session.GetTable<PocoWithCompositeKey>();
            table.Create();
            List<Guid> listOfGuids = new List<Guid>() { new Guid(), new Guid() };

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithCompositeKey pocoWithCustomAttributes = new PocoWithCompositeKey
            {
                ListOfGuids = listOfGuids,
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                IgnoredString = Guid.NewGuid().ToString(),
            };

            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithCompositeKey> records = cqlClient.Fetch<PocoWithCompositeKey>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, records[0].SomePartitionKey1);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, records[0].SomePartitionKey2);
            Assert.AreEqual(pocoWithCustomAttributes.ListOfGuids, records[0].ListOfGuids);
            Assert.AreEqual(new PocoWithCompositeKey().IgnoredString, records[0].IgnoredString);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, rows[0].GetValue<string>("somepartitionkey1"));
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, rows[0].GetValue<string>("somepartitionkey2"));
            Assert.AreEqual(pocoWithCustomAttributes.ListOfGuids, rows[0].GetValue<List<Guid>>("listofguids"));
            Assert.AreEqual(null, rows[0].GetValue<string>("ignoredstring"));
        }

        /// <summary>
        /// Attempt to insert Poco object which leaves part of the composite key null.  
        /// Expect the CqlClient to fail silently
        /// </summary>
        [Test]
        public void Attributes_CompositeKey_FirstPartOfKeyNull()
        {
            Table<PocoWithCompositeKey> table = _session.GetTable<PocoWithCompositeKey>();
            table.Create();
            List<Guid> listOfGuids = new List<Guid>() { new Guid(), new Guid() };

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithCompositeKey pocoWithCustomAttributes = new PocoWithCompositeKey
            {
                ListOfGuids = listOfGuids,
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = null,
                IgnoredString = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithCompositeKey> records = cqlClient.Fetch<PocoWithCompositeKey>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(0, records.Count);
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(0, rows.Count);
        }

        /// <summary>
        /// Attempt to insert Poco object which leaves all values equal to null.  
        /// Expect the CqlClient to fail silently
        /// </summary>
        [Test]
        public void Attributes_CompositeKey_AllFieldsNull()
        {
            Table<PocoWithCompositeKey> table = _session.GetTable<PocoWithCompositeKey>();
            table.Create();
            List<Guid> listOfGuids = new List<Guid>() { new Guid(), new Guid() };

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithCompositeKey pocoWithCustomAttributes = new PocoWithCompositeKey
            {
                ListOfGuids = null,
                SomePartitionKey1 = null,
                SomePartitionKey2 = null,
                IgnoredString = null,
            };
            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithCompositeKey> records = cqlClient.Fetch<PocoWithCompositeKey>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(0, records.Count);
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(0, rows.Count);
        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_MultipleClusteringKeys()
        {
            Table<PocoWithClusteringKeys> table = _session.GetTable<PocoWithClusteringKeys>();
            table.Create();

            var cqlClient = CqlClientConfiguration.ForSession(_session).BuildCqlClient();
            PocoWithClusteringKeys pocoWithCustomAttributes = new PocoWithClusteringKeys
            {
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                Guid1 = Guid.NewGuid(),
                Guid2 = Guid.NewGuid(),
            };

            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<PocoWithClusteringKeys> records = cqlClient.Fetch<PocoWithClusteringKeys>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, records[0].SomePartitionKey1);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, records[0].SomePartitionKey2);
            Assert.AreEqual(pocoWithCustomAttributes.Guid1, records[0].Guid1);
            Assert.AreEqual(pocoWithCustomAttributes.Guid2, records[0].Guid2);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, rows[0].GetValue<string>("somepartitionkey1"));
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, rows[0].GetValue<string>("somepartitionkey2"));
            Assert.AreEqual(pocoWithCustomAttributes.Guid1, rows[0].GetValue<Guid>("guid1"));
            Assert.AreEqual(pocoWithCustomAttributes.Guid2, rows[0].GetValue<Guid>("guid2"));
        }



        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        [Cassandra.Data.Linq.Table("pocowithignoredattributes")]
        private class PocoWithIgnoredAttributes
        {
            [Cassandra.Data.Linq.PartitionKeyAttribute]
            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Data.Linq.Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            [Cassandra.Data.Linq.Column("somenonignoreddouble")]
            public double SomeNonIgnoredDouble = 123456;

            [Cassandra.Mapping.Attributes.Ignore]
            [Cassandra.Data.Linq.Column("this_should_be_ignored")]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

        /// <summary>
        /// See PocoWithIgnoredAttributes for correctly implemented counterpart
        /// </summary>
        [Cassandra.Data.Linq.Table("pocowithwrongfieldlabeledpartitionkey")]
        private class PocoWithWrongFieldLabeledPartitionKey
        {
            [Cassandra.Data.Linq.PartitionKeyAttribute]
            [Cassandra.Data.Linq.Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            [Cassandra.Data.Linq.Column("somenonignoreddouble")]
            public double SomeNonIgnoredDouble = 123456;

            [Cassandra.Mapping.Attributes.PartitionKey]
            [Cassandra.Data.Linq.Column("someotherstring")]
            public string SomeOtherString = "someOtherString";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoWithOnlyPartitionKeyNotLabeled
        {
            public string SomeString = "somestring_value";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted, as well as another field that is not labeled
        /// </summary>
        private class PocoWithPartitionKeyNotLabeledAndOtherField
        {
            public string SomeString = "somestring_value";
            public string SomeOtherString = "someotherstring_value";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoMislabeledClusteringKey
        {
            [Cassandra.Mapping.Attributes.ClusteringKey]
            public string SomeString = "someStringValue";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoWithPartitionKeyOmitted
        {
            public double SomeDouble = (double)123456;
            public List<string> SomeList = new List<string>();
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key included, which was missing from PocoWithPartitionKeyOmitted
        /// </summary>
        private class PocoWithPartitionKeyIncluded
        {
            [Cassandra.Mapping.Attributes.PartitionKey]
            public string SomeString = "somePartitionKeyDefaultValue";
            public double SomeDouble = (double)123456;
            public List<string> SomeList = new List<string>();
        }

        /// <summary>
        /// Class designed to fix the issue with PocoWithPartitionKeyOmitted, which is implied by the name
        /// </summary>
        class PocoWithPartitionKeyIncludedMapping : Map<PocoWithPartitionKeyIncluded>
        {
            public PocoWithPartitionKeyIncludedMapping()
            {
                TableName(typeof(PocoWithPartitionKeyOmitted).Name.ToLower());
                PartitionKey(u => u.SomeString);
                Column(u => u.SomeString, cm => cm.WithName("somestring"));
            }
        }

        [Cassandra.Data.Linq.Table("pocowithcompositekey")]
        private class PocoWithCompositeKey
        {
            [Cassandra.Data.Linq.PartitionKeyAttribute(1)]
            [Cassandra.Mapping.Attributes.PartitionKey(1)]
            [Cassandra.Data.Linq.Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [Cassandra.Data.Linq.PartitionKeyAttribute(2)]
            [Cassandra.Mapping.Attributes.PartitionKey(2)]
            [Cassandra.Data.Linq.Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [Cassandra.Data.Linq.Column("listofguids")]
            public List<Guid> ListOfGuids;

            [Cassandra.Mapping.Attributes.Ignore]
            [Cassandra.Data.Linq.Column("ignoredstring")]
            public string IgnoredString = "someIgnoredString_val";
        }

        [Cassandra.Data.Linq.Table("pocowithclusteringkeys")]
        private class PocoWithClusteringKeys
        {
            [Cassandra.Data.Linq.PartitionKeyAttribute(1)]
            [Cassandra.Mapping.Attributes.PartitionKey(1)]
            [Cassandra.Data.Linq.Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [Cassandra.Data.Linq.PartitionKeyAttribute(2)]
            [Cassandra.Mapping.Attributes.PartitionKey(2)]
            [Cassandra.Data.Linq.Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [Cassandra.Data.Linq.ClusteringKey(1)]
            [Cassandra.Mapping.Attributes.ClusteringKey(1)]
            [Cassandra.Data.Linq.Column("guid1")]
            public Guid Guid1;

            [Cassandra.Data.Linq.ClusteringKey(2)]
            [Cassandra.Mapping.Attributes.ClusteringKey(2)]
            [Cassandra.Data.Linq.Column("guid2")]
            public Guid Guid2;


        }



    }
}
