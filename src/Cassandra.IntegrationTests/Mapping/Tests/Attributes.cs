using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using NUnit.Framework;
#pragma warning disable 169
#pragma warning disable 618
#pragma warning disable 612
using Linq = Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Attributes : SharedClusterTest
    {
        ISession _session;
        string _uniqueKsName;
        private const string IgnoredStringAttribute = "ignoredstringattribute";

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        private Linq::Table<T> GetTable<T>()
        {
            return new Linq::Table<T>(_session, new MappingConfiguration());
        }

        private IMapper GetMapper()
        {
            return new Mapper(_session, new MappingConfiguration());
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the field marked with mapping attribute "Ignore"
        /// </summary>
        [Test]
        public void Attributes_Ignore_TableCreatedWithMappingAttributes()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithIgnoredAttributes));
            var table = new Linq::Table<PocoWithIgnoredAttributes>(_session, new MappingConfiguration().Define(definition)); 
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.CreateIfNotExists();

            //var mapper = new Mapper(_session, new MappingConfiguration().Define(definition));
            var mapper = new Mapper(_session, new MappingConfiguration());
            var pocoToUpload = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            mapper.Insert(pocoToUpload);
            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"somepartitionkey\"='{pocoToUpload.SomePartitionKey}'";

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = mapper.Fetch<PocoWithIgnoredAttributes>(cqlSelect).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoToUpload.SomePartitionKey, records[0].SomePartitionKey);
            var defaultPoco = new PocoWithIgnoredAttributes();
            Assert.AreNotEqual(defaultPoco.IgnoredStringAttribute, pocoToUpload.IgnoredStringAttribute);
            Assert.AreEqual(defaultPoco.IgnoredStringAttribute, records[0].IgnoredStringAttribute);
            Assert.AreEqual(defaultPoco.SomeNonIgnoredDouble, records[0].SomeNonIgnoredDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoToUpload.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
            Assert.AreEqual(pocoToUpload.SomeNonIgnoredDouble, rows[0].GetValue<double>("somenonignoreddouble"));

            // Verify there was no column created for the ignored column
            var e = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>(IgnoredStringAttribute));
            var expectedErrMsg = "Column " + IgnoredStringAttribute + " not found";
            Assert.AreEqual(expectedErrMsg, e.Message);
        }
        /// <summary>
        /// Validate that the mapping mechanism ignores the field marked with mapping attribute "Ignore"
        /// </summary>
        [Test]
        public void Attributes_Ignore()
        {
            var table = GetTable<PocoWithIgnoredAttributes>();
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = GetMapper();
            var pocoToUpload = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            mapper.Insert(pocoToUpload);
            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"{"somepartitionkey"}\"='{pocoToUpload.SomePartitionKey}'";

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = mapper.Fetch<PocoWithIgnoredAttributes>(cqlSelect).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoToUpload.SomePartitionKey, records[0].SomePartitionKey);
            var defaultPoco = new PocoWithIgnoredAttributes();
            Assert.AreNotEqual(defaultPoco.IgnoredStringAttribute, pocoToUpload.IgnoredStringAttribute);
            Assert.AreEqual(defaultPoco.IgnoredStringAttribute, records[0].IgnoredStringAttribute);
            Assert.AreEqual(defaultPoco.SomeNonIgnoredDouble, records[0].SomeNonIgnoredDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoToUpload.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
            Assert.AreEqual(pocoToUpload.SomeNonIgnoredDouble, rows[0].GetValue<double>("somenonignoreddouble"));

            // Verify there was no column created for the ignored column
            var e = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>(IgnoredStringAttribute));
            var expectedErrMsg = "Column " + IgnoredStringAttribute + " not found";
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the class variable marked as "Ignore"
        /// The fact that the request does not fail trying to find a non-existing custom named column proves that 
        /// the request is not looking for the column for reads or writes.
        /// 
        /// This also validates that attributes from  Cassandra.Mapping and Cassandra.Data.Lync can be used successfully on the same object
        /// </summary>
        [Test]
        public void Attributes_Ignore_LinqAndMappingAttributes()
        {
            var config = new MappingConfiguration();
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(
                typeof(PocoWithIgnrdAttr_LinqAndMapping), 
                () => Linq::LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(PocoWithIgnrdAttr_LinqAndMapping)));
            var table = new Linq::Table<PocoWithIgnrdAttr_LinqAndMapping>(_session, config);
            table.Create();

            var cqlClient = GetMapper();
            var pocoToInsert = new PocoWithIgnrdAttr_LinqAndMapping
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(pocoToInsert);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithIgnrdAttr_LinqAndMapping>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoToInsert.SomePartitionKey, records[0].SomePartitionKey);
            var defaultPoco = new PocoWithIgnrdAttr_LinqAndMapping();
            Assert.AreEqual(defaultPoco.IgnoredStringAttribute, records[0].IgnoredStringAttribute);
            Assert.AreEqual(defaultPoco.SomeNonIgnoredDouble, records[0].SomeNonIgnoredDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoToInsert.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
            Assert.AreEqual(pocoToInsert.SomeNonIgnoredDouble, rows[0].GetValue<double>("somenonignoreddouble"));
            Assert.AreEqual(null, rows[0].GetValue<string>(IgnoredStringAttribute));
        }

        /// <summary>
        /// Verify that inserting a mapped object that totally omits the Cassandra.Mapping.Attributes.PartitionKey silently fails.
        /// However, using mapping and a different Poco that has the key, records can be inserted and fetched into the same table
        /// </summary>
        [Test]
        public void Attributes_InsertFailsWhenPartitionKeyAttributeOmitted_FixedWithMapping()
        {
            // Setup
            var tableName = typeof(PocoWithPartitionKeyOmitted).Name.ToLower();
            var selectAllCql = "SELECT * from " + tableName;
            var stringList = new List<string>() { "string1", "string2" };
            var createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, somelist list<varchar>, somedouble double)";
            _session.Execute(createTableCql);

            // Instantiate CqlClient with mapping rule that resolves the missing key issue
            var cqlClientWithMappping = new Mapper(_session, new MappingConfiguration().Define(new PocoWithPartitionKeyIncludedMapping()));
            // insert new record
            var pocoWithCustomAttributesKeyIncluded = new PocoWithPartitionKeyIncluded();
            pocoWithCustomAttributesKeyIncluded.SomeList = stringList; // make it not empty
            cqlClientWithMappping.Insert(pocoWithCustomAttributesKeyIncluded);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records1.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, records1[0].SomeString);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeList, records1[0].SomeList);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, records1[0].SomeDouble);
            records1.Clear();

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute(selectAllCql).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, rows[0].GetValue<string>("somestring"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeList, rows[0].GetValue<List<string>>("somelist"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, rows[0].GetValue<double>("somedouble"));

            // try to Select new record using poco that does not contain partition key, validate that the mapping mechanism matches what it can
            var cqlClientNomapping = GetMapper();
            var records2 = cqlClientNomapping.Fetch<PocoWithPartitionKeyOmitted>(selectAllCql).ToList();
            Assert.AreEqual(1, records2.Count);
            records2.Clear();

            // try again with the old CqlClient instance
            records1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records1.Count);

            // Clear out the table, verify
            var truncateCql = "TRUNCATE " + tableName;
            _session.Execute(truncateCql);
            records1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(0, records1.Count); // should have gone from 1 to 0 records
        }

        /// <summary>
        /// Verify that we can use a mapped object to insert / query into a table 
        /// if we leave out a column that is not the partition key (in this case the list column)
        /// </summary>
        [Test]
        public void Attributes_NonPartitionKeyFieldOmittedFromPocoClass()
        {
            var tableName = typeof(PocoWithPartitionKeyIncluded).Name.ToLower();
            var selectAllCql = "SELECT * from " + tableName;
            var createTableCql =
                $"Create table {tableName}(somestring text PRIMARY KEY, somelist list<text>, somelist2 list<text>, somedouble double)";
            _session.Execute(createTableCql);
            var cqlClient = GetMapper();

            // insert new record
            var pocoWithCustomAttributesKeyIncluded = new PocoWithPartitionKeyIncluded();
            cqlClient.Insert(pocoWithCustomAttributesKeyIncluded);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, records[0].SomeString);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, records[0].SomeDouble);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute(selectAllCql).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeString, rows[0].GetValue<string>("somestring"));
            Assert.AreEqual(pocoWithCustomAttributesKeyIncluded.SomeDouble, rows[0].GetValue<double>("somedouble"));
        }

        /// <summary>
        /// Verify that inserting a mapped object without specifying Cassandra.Mapping.Attributes.PartitionKey does not fail
        /// This also validates that not all columns need to be included for the Poco insert / fetch to succeed
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_PartitionKeyNotLabeled()
        {
            var tableName = typeof(PocoWithOnlyPartitionKeyNotLabeled).Name.ToLower();
            var createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, somelist list<text>, somedouble double)";
            _session.Execute(createTableCql);

            var cqlClient = GetMapper();
            var pocoWithOnlyCustomAttributes = new PocoWithOnlyPartitionKeyNotLabeled();
            cqlClient.Insert(pocoWithOnlyCustomAttributes); 

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithOnlyPartitionKeyNotLabeled>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithOnlyCustomAttributes.SomeString, records[0].SomeString);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute("SELECT * from " + tableName).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithOnlyCustomAttributes.SomeString, rows[0].GetValue<string>("somestring"));
        }

        /// <summary>
        /// Verify that inserting a mapped object without including PartitionKey succeeds when it is not the only field in the Poco class
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_PartitionKeyNotLabeled_AnotherNonLabelFieldIncluded()
        {
            var tableName = typeof(PocoWithPartitionKeyNotLabeledAndOtherField).Name.ToLower();
            var createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY, someotherstring text, somelist list<text>, somedouble double)";
            _session.Execute(createTableCql);

            var cqlClient = GetMapper();
            var pocoWithOnlyCustomAttributes = new PocoWithPartitionKeyNotLabeledAndOtherField();
            cqlClient.Insert(pocoWithOnlyCustomAttributes); 

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithPartitionKeyNotLabeledAndOtherField>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
        }


        /// <summary>
        /// Verify that inserting a mapped object, mislabeling the PartitionKey as a Clustering Key does not fail
        /// </summary>
        [Test]
        public void Attributes_MislabledClusteringKey()
        {
            var tableName = typeof(PocoMislabeledClusteringKey).Name.ToLower();
            var createTableCql = "Create table " + tableName + "(somestring varchar PRIMARY KEY)";
            _session.Execute(createTableCql);

            var cqlClient = GetMapper();
            var pocoWithCustomAttributes = new PocoMislabeledClusteringKey();
            cqlClient.Insert(pocoWithCustomAttributes); // TODO: Should this fail?

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoMislabeledClusteringKey>("SELECT * from " + tableName).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomeString, records[0].SomeString);

        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_CompositeKey()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithCompositeKey));
            var table = new Linq::Table<PocoWithCompositeKey>(_session, new MappingConfiguration().Define(definition));
            table.Create();

            var listOfGuids = new List<Guid>() { new Guid(), new Guid() };

            var mapper = new Mapper(_session, new MappingConfiguration().Define(definition));
            var pocoWithCustomAttributes = new PocoWithCompositeKey
            {
                ListOfGuids = listOfGuids,
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                IgnoredString = Guid.NewGuid().ToString(),
            };

            mapper.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = mapper.Fetch<PocoWithCompositeKey>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, records[0].SomePartitionKey1);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, records[0].SomePartitionKey2);
            Assert.AreEqual(pocoWithCustomAttributes.ListOfGuids, records[0].ListOfGuids);
            Assert.AreEqual(new PocoWithCompositeKey().IgnoredString, records[0].IgnoredString);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, rows[0].GetValue<string>("somepartitionkey1"));
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, rows[0].GetValue<string>("somepartitionkey2"));
            Assert.AreEqual(pocoWithCustomAttributes.ListOfGuids, rows[0].GetValue<List<Guid>>("listofguids"));
            var ex = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>("ignoredstring"));
            Assert.AreEqual("Column ignoredstring not found", ex.Message);
        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_MultipleClusteringKeys()
        {
            var config = new MappingConfiguration();
            config.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(PocoWithClusteringKeys), 
                () => Linq::LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(PocoWithClusteringKeys)));
            var table = new Linq::Table<PocoWithClusteringKeys>(_session, config);
            table.Create();

            var cqlClient = new Mapper(_session, config);

            var pocoWithCustomAttributes = new PocoWithClusteringKeys
            {
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                Guid1 = Guid.NewGuid(),
                Guid2 = Guid.NewGuid(),
            };

            cqlClient.Insert(pocoWithCustomAttributes);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithClusteringKeys>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, records[0].SomePartitionKey1);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, records[0].SomePartitionKey2);
            Assert.AreEqual(pocoWithCustomAttributes.Guid1, records[0].Guid1);
            Assert.AreEqual(pocoWithCustomAttributes.Guid2, records[0].Guid2);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey1, rows[0].GetValue<string>("somepartitionkey1"));
            Assert.AreEqual(pocoWithCustomAttributes.SomePartitionKey2, rows[0].GetValue<string>("somepartitionkey2"));
            Assert.AreEqual(pocoWithCustomAttributes.Guid1, rows[0].GetValue<Guid>("guid1"));
            Assert.AreEqual(pocoWithCustomAttributes.Guid2, rows[0].GetValue<Guid>("guid2"));
        }

        /// <summary>
        /// Expect a "missing partition key" failure upon create since there was no field specific to the class being created
        /// that was marked as partition key.
        /// This happens despite the matching partition key names since they reside in different classes.
        /// </summary>
        [Test]
        public void Attributes_Mapping_MisMatchedClassTypesButTheSamePartitionKeyName()
        {
            var mapping = new Map<SimplePocoWithPartitionKey>();
            mapping.CaseSensitive();
            mapping.PartitionKey(u => u.StringType);
            var table = new Linq::Table<ManyDataTypesPoco>(_session, new MappingConfiguration().Define(mapping));

            // Validate expected Exception
            var ex = Assert.Throws<InvalidOperationException>(table.Create);
            StringAssert.Contains("No partition key defined", ex.Message);
        }

        /// <summary>
        /// The Partition key Attribute from the Poco class is used to create a table with a partition key
        /// </summary>
        [Test]
        public void Attributes_ClusteringKey_NoName()
        {
            var table = GetTable<EmptyClusteringColumnName>();
            table.Create();
            var definition = new AttributeBasedTypeDefinition(typeof(EmptyClusteringColumnName));
            var mapper = new Mapper(_session, new MappingConfiguration().Define(definition));
            var pocoToUpload = new EmptyClusteringColumnName
            {
                Id = 1,
                cluster = "c2",
                value = "v2"
            };
            mapper.Insert(pocoToUpload);
            var cqlSelect = $"SELECT * from {table.Name} where id={pocoToUpload.Id}";
            var instancesQueried = mapper.Fetch<EmptyClusteringColumnName>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            Assert.AreEqual(pocoToUpload.Id, instancesQueried[0].Id);
            Assert.AreEqual(pocoToUpload.cluster, instancesQueried[0].cluster);
            Assert.AreEqual(pocoToUpload.value, instancesQueried[0].value);
        }

        /// <summary>
        /// The Partition key Attribute from the Poco class is used to create a table with a partition key
        /// </summary>
        [Test]
        public void Attributes_PartitionKey()
        {
            var table = GetTable<SimplePocoWithPartitionKey>();
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var cqlClient = GetMapper();
            var pocoToUpload = new SimplePocoWithPartitionKey();
            cqlClient.Insert(pocoToUpload);
            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"{"stringtype"}\"='{pocoToUpload.StringType}'";
            var instancesQueried = cqlClient.Fetch<SimplePocoWithPartitionKey>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            Assert.AreEqual(pocoToUpload.StringType, instancesQueried[0].StringType);
            Assert.AreEqual(pocoToUpload.StringTyp, instancesQueried[0].StringTyp);
            Assert.AreEqual(pocoToUpload.StringTypeNotPartitionKey, instancesQueried[0].StringTypeNotPartitionKey);
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Partition key Attribute from 
        /// the Poco class it's derived from
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_SecondaryIndex()
        {
            var table = GetTable<SimplePocoWithSecondaryIndex>();
            table.Create();

            var cqlClient = GetMapper();
            var expectedTotalRecords = 10;
            var defaultInstance = new SimplePocoWithSecondaryIndex();
            for (var i = 0; i < expectedTotalRecords; i++)
                cqlClient.Insert(new SimplePocoWithSecondaryIndex(i));
            var instancesQueried = cqlClient.Fetch<SimplePocoWithSecondaryIndex>().ToList();
            Assert.AreEqual(expectedTotalRecords, instancesQueried.Count);

            // Select using basic cql
            var cqlSelect =
                $"SELECT * from \"{table.Name.ToLower()}\" where {"somesecondaryindex"}={defaultInstance.SomeSecondaryIndex} order by {"somepartitionkey"} desc";
            var ex = Assert.Throws<InvalidQueryException>(() => cqlClient.Fetch<SimplePocoWithSecondaryIndex>(cqlSelect));
            Assert.AreEqual("ORDER BY with 2ndary indexes is not supported.", ex.Message);
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Column Attribute from 
        /// the Poco class it's derived from
        /// </summary>
        [Test]
        public void Attributes_Column_NoCustomLabel()
        {
            // Setup
            var expectedTotalRecords = 1;
            var definition = new AttributeBasedTypeDefinition(typeof(SimplePocoWithColumnAttribute));
            var table = new Linq::Table<SimplePocoWithColumnAttribute>(_session, new MappingConfiguration().Define(definition));
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var defaultInstance = new SimplePocoWithColumnAttribute();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(definition));
            mapper.Insert(defaultInstance);

            // Validate using mapped Fetch
            var cqlSelectAll = "select * from " + table.Name.ToLower();
            var instancesQueried = mapper.Fetch<SimplePocoWithColumnAttribute>(cqlSelectAll).ToList();
            Assert.AreEqual(expectedTotalRecords, instancesQueried.Count);

            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where {"somepartitionkey"}='{defaultInstance.SomePartitionKey}'";
            var actualObjectsInOrder = mapper.Fetch<SimplePocoWithColumnAttribute>(cqlSelect).ToList();
            Assert.AreEqual(expectedTotalRecords, actualObjectsInOrder.Count);

            // Validate using straight cql to verify column names
            var rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(expectedTotalRecords, rows.Count);
            Assert.AreEqual(defaultInstance.SomeColumn, rows[0].GetValue<int>("somecolumn"));
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Column Attribute from 
        /// the Poco class it's derived from, including the custom label option
        /// </summary>
        [Test, TestCassandraVersion(2,0)]
        public void Attributes_Column_CustomLabels()
        {
            // Setup
            var expectedTotalRecords = 1;
            var definition = new AttributeBasedTypeDefinition(typeof(SimplePocoWithColumnLabel_CustomColumnName));
            var table = new Linq::Table<SimplePocoWithColumnLabel_CustomColumnName>(_session, new MappingConfiguration().Define(definition));
            Assert.AreEqual(typeof(SimplePocoWithColumnLabel_CustomColumnName).Name, table.Name); // Assert table name is case sensitive now
            Assert.AreNotEqual(typeof(SimplePocoWithColumnLabel_CustomColumnName).Name, typeof(SimplePocoWithColumnLabel_CustomColumnName).Name.ToLower()); // Assert table name is case senstive
            table.Create();

            var defaultInstance = new SimplePocoWithColumnLabel_CustomColumnName();
            var mapper = new Mapper(_session, new MappingConfiguration().Define(definition));
            mapper.Insert(defaultInstance);

            // Validate using mapped Fetch
            var cqlSelect =
                $"SELECT * from \"{table.Name.ToLower()}\" where {"someCaseSensitivePartitionKey"}='{defaultInstance.SomePartitionKey}'";
            var actualObjectsInOrder = mapper.Fetch<SimplePocoWithColumnLabel_CustomColumnName>(cqlSelect).ToList();
            Assert.AreEqual(expectedTotalRecords, actualObjectsInOrder.Count);
            Assert.AreEqual(defaultInstance.SomeColumn, actualObjectsInOrder[0].SomeColumn);

            // Validate using straight cql to verify column names
            var rows = _session.Execute(cqlSelect).GetRows().ToList();
            Assert.AreEqual(expectedTotalRecords, rows.Count);
            Assert.AreEqual(defaultInstance.SomeColumn, rows[0].GetValue<int>("some_column_label_thats_different"));
        }

        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        [Table("SimplePocoWithColumnLabel_CustomColumnName")]
        public class SimplePocoWithColumnLabel_CustomColumnName
        {
            [Column("someCaseSensitivePartitionKey")]
            [PartitionKey]
            public string SomePartitionKey = "defaultPartitionKeyVal";
            [Column("some_column_label_thats_different")]
            public int SomeColumn = 191991919;
        }

        public class SimplePocoWithColumnAttribute
        {
            [PartitionKey]
            public string SomePartitionKey = "defaultPartitionKeyVal";
            [Column]
            public int SomeColumn = 121212121;
        }


        public class SimplePocoWithSecondaryIndex
        {
            [PartitionKey]
            public string SomePartitionKey;
            [SecondaryIndex]
            public int SomeSecondaryIndex = 1;

            public SimplePocoWithSecondaryIndex() { }

            public SimplePocoWithSecondaryIndex(int i)
            {
                SomePartitionKey = "partitionKey_" + i;
            }
        }

        private class SimplePocoWithPartitionKey
        {
            public string StringTyp = "someStringValue";
            [PartitionKey]
            public string StringType = "someStringValue";
            public string StringTypeNotPartitionKey = "someStringValueNotPk";
        }

        private class PocoWithIgnoredAttributes
        {
            [PartitionKey]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";
            public double SomeNonIgnoredDouble = 123456;
            [Cassandra.Mapping.Attributes.Ignore]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

        /// <summary>
        /// Test poco class that uses both Linq and Cassandra.Mapping attributes at the same time
        /// </summary>
        [Linq::Table("pocowithignrdattr_linqandmapping")]
        private class PocoWithIgnrdAttr_LinqAndMapping
        {
            [Linq::PartitionKey]
            [PartitionKey]
            [Linq::Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            [Linq::Column("somenonignoreddouble")]
            public double SomeNonIgnoredDouble = 123456;

            [Cassandra.Mapping.Attributes.Ignore]
            [Linq::Column(Attributes.IgnoredStringAttribute)]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

        /// <summary>
        /// See PocoWithIgnoredAttributes for correctly implemented counterpart
        /// </summary>
        [Linq::Table("pocowithwrongfieldlabeledpk")]
        private class PocoWithWrongFieldLabeledPk
        {
            [Linq::PartitionKey]
            [Linq::Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            [Linq::Column("somenonignoreddouble")]
            public double SomeNonIgnoredDouble = 123456;

            [PartitionKey]
            [Linq::Column("someotherstring")]
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
            [ClusteringKey]
            public string SomeString = "someStringValue";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoWithPartitionKeyOmitted
        {
            public double SomeDouble = 123456;
            public List<string> SomeList = new List<string>();
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key included, which was missing from PocoWithPartitionKeyOmitted
        /// </summary>
        private class PocoWithPartitionKeyIncluded
        {
            [PartitionKey]
            public string SomeString = "somePartitionKeyDefaultValue";
            public double SomeDouble = 123456;
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

        [Linq::Table("pocowithcompositekey")]
        private class PocoWithCompositeKey
        {
            [Linq::PartitionKey(1)]
            [PartitionKey(1)]
            [Linq::Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [Linq::PartitionKey(2)]
            [PartitionKey(2)]
            [Linq::Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [Linq::Column("listofguids")]
            public List<Guid> ListOfGuids;

            [Cassandra.Mapping.Attributes.Ignore]
            [Linq::Column("ignoredstring")]
            public string IgnoredString = "someIgnoredString_val";
        }

        [Linq::Table("pocowithclusteringkeys")]
        private class PocoWithClusteringKeys
        {
            [Linq::PartitionKey(1)]
            [PartitionKey(1)]
            [Linq::Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [Linq::PartitionKey(2)]
            [PartitionKey(2)]
            [Linq::Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [Linq::ClusteringKey(1)]
            [ClusteringKey(1)]
            [Linq::Column("guid1")]
            public Guid Guid1;

            [Linq::ClusteringKey(2)]
            [ClusteringKey(2)]
            [Linq::Column("guid2")]
            public Guid Guid2;
        }
    }
}
