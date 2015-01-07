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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    [Category("short")]
    public class CreateTable : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(CreateTable));
        string _uniqueKsName;
        private ITestCluster _testCluster;

        [SetUp]
        public void SetupTest()
        {
            _testCluster = TestClusterManager.GetTestCluster(1);
            _session = _testCluster.Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            try
            {
                _session.DeleteKeyspace(_uniqueKsName);
            }
            catch (InvalidConfigurationInQueryException) {} // keyspace has already been dropped
            // _testCluster.Cluster.Shutdown(); // avoid keyspace switching issues
        }

        /// <summary>
        /// Create a table using the method CreateIfNotExists
        /// 
        /// @Jira CSHARP-42  https://datastax-oss.atlassian.net/browse/CSHARP-42
        ///  - Jira detail: CreateIfNotExists causes InvalidOperationException
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExist()
        {
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create
        /// </summary>
        [Test]
        public void TableCreate_Create()
        {
            // Test
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create, 
        /// overriding the default table name using fluent mapping
        /// Validate default table casing rules
        /// </summary>
        [Test]
        public void TableCreate_Create_NameOverride_DefaultLowercase()
        {
            // Test
            string uniqueTableName = TestUtils.GetUniqueTableName();
            Assert.AreNotEqual(uniqueTableName.ToLower(), uniqueTableName);
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName).PartitionKey(c => c.StringType));
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);

            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);

            // Assert resultant state of table
            Assert.AreEqual(uniqueTableName, table.Name);
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName.ToLower()));
            Assert.IsFalse(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName));
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create, 
        /// overriding the default table name using fluent mapping, enforcing case-sensitivity
        /// Validate default table casing rules
        /// </summary>
        [Test]
        public void TableCreate_Create_NameOverride_CaseSensitive()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName();
            Assert.AreNotEqual(uniqueTableName.ToLower(), uniqueTableName);
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);

            // Test
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);

            // Assert resultant state of table
            Assert.AreEqual(uniqueTableName, table.Name);
            Assert.IsFalse(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName.ToLower()));
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName));
            AllDataTypesEntity.WriteReadValidate(table);
        }


        /// <summary>
        /// Attempt to create the same table using the method Create twice, validate expected failure message
        /// </summary>
        [Test]
        public void TableCreate_CreateTable_AlreadyExists()
        {
            // Setup / Test
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            TableAttribute tableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(AllDataTypesEntity), typeof(TableAttribute));
            table.Create();

            var e = Assert.Throws<AlreadyExistsException>(() => table.Create());
            Assert.AreEqual(string.Format("Table {0}.{1} already exists", _uniqueKsName, tableAttribute.Name), e.Message);
        }

        /// <summary>
        /// Attempt to create two tables of different types but with the same name using the Create method. 
        /// Validate expected failure message
        /// </summary>
        [Test]
        public void TableCreate_CreateTable_SameNameDifferentTypeAlreadyExists()
        {
            // First table name creation works as expected
            Table<AllDataTypesEntity> allDataTypesTable = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            allDataTypesTable.Create();
            TableAttribute allDataTypesTableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(AllDataTypesEntity), typeof(TableAttribute));

            // Second creation attempt with same table name should fail
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(allDataTypesTableAttribute.Name).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);

            // Test
            var e = Assert.Throws<AlreadyExistsException>(() => table.Create());
            Assert.AreEqual(string.Format("Table {0}.{1} already exists", _uniqueKsName, allDataTypesTableAttribute.Name), e.Message);
        }


        /// <summary>
        /// Successfully create two tables of the same type in the same keyspace, but with different names
        /// </summary>
        [Test]
        public void TableCreate_Create_TwoTablesWithSameMappedType_DifferentNames()
        {
            // Create and test first table
            string uniqueTableName1 = TestUtils.GetUniqueTableName();
            var mappingConfig1 = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName1).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table1 = new Table<AllDataTypesEntity>(_session, mappingConfig1);
            table1.Create();
            AllDataTypesEntity.WriteReadValidate(table1);

            // Create and test second table
            string uniqueTableName2 = TestUtils.GetUniqueTableName();
            var mappingConfig2 = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName2).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table2 = new Table<AllDataTypesEntity>(_session, mappingConfig2);
            table2.Create();
            AllDataTypesEntity.WriteReadValidate(table2);

            Assert.AreNotEqual(uniqueTableName1, uniqueTableName2);
            Assert.AreNotEqual(table1.Name, table2.Name);
            Assert.AreEqual(uniqueTableName1, table1.Name);
            Assert.AreEqual(uniqueTableName2, table2.Name);
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName1));
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName2));
        }

        /// <summary>
        /// Successfully re-create a recently deleted table using the method Create, 
        /// all using the same Session instance
        /// </summary>
        [Test]
        public void TableCreate_ReCreateTableAfterDropping()
        {
            // Setup
            TableAttribute tableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(AllDataTypesEntity), typeof(TableAttribute));
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());

            // Create then delete then cre-create the table
            table.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
            _session.Execute(string.Format("DROP TABLE \"{0}\".\"{1}\"", _uniqueKsName, tableAttribute.Name));
            Assert.IsFalse(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
            table.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
        }

        /// <summary>
        /// Attempt to call table.Create() a table in a keyspace that was recently deleted
        /// Validate error message.
        /// </summary>
        [Test]
        public void TableCreate_Create_KeyspaceDeleted()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName();
            string expectedErrMsg = string.Format("Cannot add column family '{0}' to non existing keyspace '{1}'.", uniqueTableName, _uniqueKsName);
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);

            // Delete keyspace before attempting to create the table
            _session.DeleteKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);

            var e = Assert.Throws<InvalidConfigurationInQueryException>(() => table.Create());
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Attempt to call table.CreateIfNotExists() a table in a keyspace that was recently deleted
        /// Validate error message.
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExists_KeyspaceDeleted()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName();
            string expectedErrMsg = string.Format("Cannot add column family '{0}' to non existing keyspace '{1}'.", uniqueTableName, _uniqueKsName);
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(uniqueTableName).PartitionKey(c => c.StringType).CaseSensitive());
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);

            // Delete keyspace before attempting to create the table
            _session.DeleteKeyspace(_uniqueKsName);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);

            var e = Assert.Throws<InvalidConfigurationInQueryException>(() => table.CreateIfNotExists());
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method Create
        /// </summary>
        [Test]
        public void TableCreate_Create_TwoTablesSameName_TwoDifferentKeyspaces()
        {
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);

            // Create in second keyspace
            string newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(newUniqueKsName);
            _session.ChangeKeyspace(newUniqueKsName);

            table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.Create();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method CreateIfNotExists
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExists_TwoTablesSameName_TwoDifferentKeyspaces()
        {
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            AllDataTypesEntity.WriteReadValidate(table);

            // Create in second keyspace
            string newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(newUniqueKsName);
            _session.ChangeKeyspace(newUniqueKsName);

            table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            AllDataTypesEntity.WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table that contains no column meta data using the method Create
        /// Validate the state of the table in C* after it's created
        /// </summary>
        [Test]
        public void TableCreate_Create_EntityTypeWithColumnNameMeta()
        {
            // Test
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.Create();
            AllDataTypesEntity expectedAllDataTypesEntityNoColumnMetaEntity = AllDataTypesEntity.WriteReadValidate(table);

            // Do regular CQL query, validate that correct columns names are available as RowSet keys
            string cql = string.Format("Select * from \"{0}\".\"{1}\" where \"{2}\"='{3}'", _uniqueKsName, table.Name, "string_type",
                expectedAllDataTypesEntityNoColumnMetaEntity.StringType);
            List<Row> listOfAllDataTypesObjects = _session.Execute(cql).GetRows().ToList();
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            Row row = listOfAllDataTypesObjects[0];
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.BooleanType, row["boolean_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DateTimeOffsetType.ToString(), ((DateTimeOffset)row["date_time_offset_type"]).ToString());
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DateTimeType.ToString("US"), ((DateTimeOffset)row["date_time_type"]).ToString("US"));
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DecimalType, row["decimal_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DictionaryStringLongType, row["map_type_string_long_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DictionaryStringStringType, row["map_type_string_string_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.DoubleType, row["double_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.FloatType, row["float_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.GuidType, row["guid_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.Int64Type, row["int64_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.IntType, row["int_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.ListOfGuidsType, row["list_of_guids_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.ListOfStringsType, row["list_of_strings_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.NullableIntType, row["nullable_int_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.NullableTimeUuidType, row["nullable_time_uuid_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.StringType, row["string_type"]);
            Assert.AreEqual(expectedAllDataTypesEntityNoColumnMetaEntity.TimeUuidType.ToGuid(), (Guid)row["time_uuid_type"]);
        }

        /// <summary>
        /// Successfully create a table that contains no column meta data using the method Create
        /// Validate the state of the table in C* after it's created
        /// </summary>
        [Test]
        public void TableCreate_Create_EntityTypeWithoutColumnNameMeta()
        {
            // Test
            //Table<ManyDataTypesPoco> table = new Table<ManyDataTypesPoco>(_session);

            Table<AllDataTypesNoColumnMeta> table = new Table<AllDataTypesNoColumnMeta>(_session, new MappingConfiguration());
            table.Create();
            AllDataTypesNoColumnMeta expectedAllDataTypesNoColumnMetaEntity = WriteReadValidate(table);

            // Do regular CQL query, validate that correct columns names are available as RowSet keys
            string cql = string.Format("Select * from \"{0}\".\"{1}\" where \"{2}\"='{3}'", _uniqueKsName, table.Name, "StringType",
                expectedAllDataTypesNoColumnMetaEntity.StringType);
            List<Row> listOfAllDataTypesObjects = _session.Execute(cql).GetRows().ToList();
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            Row row = listOfAllDataTypesObjects[0];
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.BooleanType, row["BooleanType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DateTimeOffsetType.ToString(), ((DateTimeOffset)row["DateTimeOffsetType"]).ToString());
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DateTimeType.ToString("US"), ((DateTimeOffset)row["DateTimeType"]).ToString("US"));
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DecimalType, row["DecimalType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DictionaryStringLongType, row["DictionaryStringLongType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DictionaryStringStringType, row["DictionaryStringStringType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.DoubleType, row["DoubleType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.FloatType, row["FloatType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.GuidType, row["GuidType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.Int64Type, row["Int64Type"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.IntType, row["IntType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.ListOfGuidsType, row["ListOfGuidsType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.ListOfStringsType, row["ListOfStringsType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.NullableIntType, row["NullableIntType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.NullableTimeUuidType, row["NullableTimeUuidType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.StringType, row["StringType"]);
            Assert.AreEqual(expectedAllDataTypesNoColumnMetaEntity.TimeUuidType.ToGuid(), (Guid)row["TimeUuidType"]);
        }

        /// <summary>
        /// Table creation fails because the referenced class is missing a partition key
        /// This also validates that a private class can be used with the Table.Create() method.
        /// </summary>
        [Test]
        public void TableCreate_ClassMissingPartitionKey()
        {
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>());
            Table<AllDataTypesEntity> table = new Table<AllDataTypesEntity>(_session, mappingConfig);
            var e =Assert.Throws<InvalidOperationException>(() => table.Create());
            Assert.AreEqual("Cannot create CREATE statement for POCO of type " + typeof(AllDataTypesEntity).Name + 
                " because it is missing PK columns id.  Are you missing a property/field on the POCO or did you forget to specify the PK columns in the mapping?", e.Message);
        }

        ///////////////////////////////////////////////
        // Test Helpers
        //////////////////////////////////////////////

        // AllDataTypes


        // AllDataTypesNoColumnMeta

        private AllDataTypesNoColumnMeta WriteReadValidate(Table<AllDataTypesNoColumnMeta> table)
        {
            WriteReadValidateUsingSessionBatch(table);
            return WriteReadValidateUsingTableMethods(table);
        }

        private AllDataTypesNoColumnMeta WriteReadValidateUsingSessionBatch(Table<AllDataTypesNoColumnMeta> table)
        {
            Batch batch = _session.CreateBatch();
            AllDataTypesNoColumnMeta expectedDataTypesRow = AllDataTypesNoColumnMeta.GetRandomInstance();
            string uniqueKey = expectedDataTypesRow.StringType;
            batch.Append(table.Insert(expectedDataTypesRow));
            batch.Execute();

            List<AllDataTypesNoColumnMeta> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesNoColumnMeta actualDataTypesRow = listOfAllDataTypesObjects.First();
            expectedDataTypesRow.AssertEquals(actualDataTypesRow);
            return expectedDataTypesRow;
        }

        private AllDataTypesNoColumnMeta WriteReadValidateUsingTableMethods(Table<AllDataTypesNoColumnMeta> table)
        {
            AllDataTypesNoColumnMeta expectedDataTypesRow = AllDataTypesNoColumnMeta.GetRandomInstance();
            string uniqueKey = expectedDataTypesRow.StringType;

            // insert record
            _session.Execute(table.Insert(expectedDataTypesRow));

            // select record
            List<AllDataTypesNoColumnMeta> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesNoColumnMeta actualDataTypesRow = listOfAllDataTypesObjects.First();
            expectedDataTypesRow.AssertEquals(actualDataTypesRow);
            return expectedDataTypesRow;
        }

    }
}
