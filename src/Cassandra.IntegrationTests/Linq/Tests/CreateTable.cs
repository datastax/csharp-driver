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
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.Tests
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
        /// Create a table using the method CreateIfNotExists
        /// 
        /// @Jira CSHARP-42  https://datastax-oss.atlassian.net/browse/CSHARP-42
        ///  - Jira detail: CreateIfNotExists causes InvalidOperationException
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExist()
        {
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.CreateIfNotExists();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create
        /// </summary>
        [Test]
        public void TableCreate_Create()
        {
            // Test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create, 
        /// overriding the default table name given via the class' "name" meta-tag
        /// </summary>
        [Test]
        public void TableCreate_Create_NameOverride()
        {
            // Test
            string uniqueTableName = TestUtils.GetUniqueTableName();
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>(uniqueTableName);
            table.Create();
            WriteReadValidate(table);

            Assert.AreEqual(uniqueTableName, table.Name);
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName));
        }

        /// <summary>
        /// Attempt to create the same table using the method Create twice, validate expected failure message
        /// </summary>
        [Test]
        public void TableCreate_CreateTable_AlreadyExists()
        {
            // Test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            TableAttribute tableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(AllDataTypesEntity), typeof(TableAttribute));
            table.Create();
            //Result Message:	Cassandra.AlreadyExistsException : Table test_cluster_keyspace.allDataTypes already exists
            try
            {
                table.Create();
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (AlreadyExistsException e)
            {
                Assert.AreEqual(string.Format("Table {0}.{1} already exists", _uniqueKsName, tableAttribute.Name), e.Message);
            }
        }

        /// <summary>
        /// Attempt to create two tables of different types but with the same name using the Create method. 
        /// Validate expected failure message
        /// </summary>
        [Test]
        public void TableCreate_CreateTable_SameNameDifferentTypeAlreadyExists()
        {
            // First table name creation works as expected
            Table<AllDataTypesEntity> allDataTypesTable = _session.GetTable<AllDataTypesEntity>();
            allDataTypesTable.Create();
            TableAttribute allDataTypesTableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(AllDataTypesEntity), typeof(TableAttribute));
            
            // Second creation attempt with same table name should fail
            Table<Movie> movieTable = _session.GetTable<Movie>(allDataTypesTableAttribute.Name);
            //Result Message:	Cassandra.AlreadyExistsException : Table test_cluster_keyspace.allDataTypes already exists
            try
            {
                movieTable.Create();
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (AlreadyExistsException e)
            {
                Assert.AreEqual(string.Format("Table {0}.{1} already exists", _uniqueKsName, allDataTypesTableAttribute.Name), e.Message);
            }
        }


        /// <summary>
        /// Successfully create two tables of the same type in the same keyspace, but with different names
        /// </summary>
        [Test]
        public void TableCreate_Create_TwoTablesWithSameMappedType_DifferentNames()
        {
            // Test
            string uniqueTableName = TestUtils.GetUniqueTableName();
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>(uniqueTableName);
            table.Create();
            WriteReadValidate(table);

            Assert.AreEqual(uniqueTableName, table.Name);
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName));
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
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();

            // Create then delete then cre-create the table
            table.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
            _session.Execute(string.Format("DROP TABLE \"{0}\".\"{1}\"", _uniqueKsName, tableAttribute.Name));
            Assert.IsFalse(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
            table.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, tableAttribute.Name));
        }

        /// <summary>
        /// Attempt to create a table in a non-existent keyspace, specifying the keyspace name in the GetTable() method's override option
        /// Validate error message.
        /// </summary>
        [Test]
        public void TableCreate_Create_KeyspaceOverride_NoSuchKeyspace()
        {
            string uniqueTableName = TestUtils.GetUniqueTableName();
            string uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            string expectedErrMsg = string.Format("Cannot add column family '{0}' to non existing keyspace '{1}'.", uniqueTableName, uniqueKsName);
            Table<AllDataTypesEntity> table1 = _session.GetTable<AllDataTypesEntity>(uniqueTableName, uniqueKsName);

            try
            {
                table1.Create();
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (InvalidConfigurationInQueryException e)
            {
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        /// <summary>
        /// Attempt to create a table in a non-existent keyspace, specifying the keyspace name in the GetTable() method's override option
        /// Validate error message.
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExists_KeyspaceOverride_NoSuchKeyspace()
        {
            string uniqueTableName = TestUtils.GetUniqueTableName();
            string uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            string expectedErrMsg = string.Format("Cannot add column family '{0}' to non existing keyspace '{1}'.", uniqueTableName, uniqueKsName);
            Table<AllDataTypesEntity> table1 = _session.GetTable<AllDataTypesEntity>(uniqueTableName, uniqueKsName);

            try
            {
                table1.CreateIfNotExists();
                Assert.Fail("Expected Exception was not thrown!");
            }
            catch (InvalidConfigurationInQueryException e)
            {
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method Create
        /// </summary>
        [Test]
        public void TableCreate_Create_TwoTablesSameName_TwoDifferentKeyspaces()
        {
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            WriteReadValidate(table);

            // Create in second keyspace
            string newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(newUniqueKsName);
            _session.ChangeKeyspace(newUniqueKsName);

            table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method CreateIfNotExists
        /// </summary>
        [Test]
        public void TableCreate_CreateIfNotExists_TwoTablesSameName_TwoDifferentKeyspaces()
        {
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.CreateIfNotExists();
            WriteReadValidate(table);

            // Create in second keyspace
            string newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(newUniqueKsName);
            _session.ChangeKeyspace(newUniqueKsName);

            table = _session.GetTable<AllDataTypesEntity>();
            table.CreateIfNotExists();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method Create
        /// Do not manually change the session to use the different keyspace
        /// </summary>
        [Test]
        public void TableCreate_CreateTable_TwoDifferentKeyspaces_KeyspaceOverrideInConstructor()
        {
            string uniqueTableNameToBeShared = TestUtils.GetUniqueTableName();
            string uniqueKsName1 = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(uniqueKsName1);
            string uniqueKsName2 = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(uniqueKsName2);

            _session.ChangeKeyspace(uniqueKsName1);
            Table<AllDataTypesEntity> table1 = _session.GetTable<AllDataTypesEntity>(uniqueTableNameToBeShared, uniqueKsName1);
            table1.Create();
            WriteReadValidate(table1);

            _session.ChangeKeyspace(uniqueKsName2);
            Table<AllDataTypesEntity> table2 = _session.GetTable<AllDataTypesEntity>(uniqueTableNameToBeShared, uniqueKsName2);
            table2.Create();
            WriteReadValidate(table2);

            _session.ChangeKeyspace(uniqueKsName1);
            WriteReadValidate(table1);
            _session.ChangeKeyspace(uniqueKsName2);
            WriteReadValidate(table2);
        }

        /// <summary>
        /// Successfully create a table that contains no column meta data using the method Create
        /// Validate the state of the table in C* after it's created
        /// </summary>
        [Test]
        public void TableCreate_Create_EntityTypeWithColumnNameMeta()
        {
            // Test
            Table<AllDataTypesEntity> table = _session.GetTable<AllDataTypesEntity>();
            table.Create();
            AllDataTypesEntity expectedAllDataTypesEntityNoColumnMetaEntity = WriteReadValidate(table);

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
            Table<AllDataTypesNoColumnMeta> table = _session.GetTable<AllDataTypesNoColumnMeta>();
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
        public void Insert_ClassMissingPartitionKey()
        {
            Table<PrivateClassMissingPartitionKey> table = _session.GetTable<PrivateClassMissingPartitionKey>();
            try
            {
                table.Create();
            }
            catch (InvalidOperationException e)
            {
                Assert.AreEqual("No partition key defined", e.Message);
            }
        }

        ///////////////////////////////////////////////
        // Test Helpers
        //////////////////////////////////////////////

        // AllDataTypes

        private AllDataTypesEntity WriteReadValidateUsingTableMethods(Table<AllDataTypesEntity> table)
        {
            AllDataTypesEntity expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            string uniqueKey = expectedDataTypesEntityRow.StringType;

            // insert record
            _session.Execute(table.Insert(expectedDataTypesEntityRow));

            // select record
            List<AllDataTypesEntity> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesEntity actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
            return expectedDataTypesEntityRow;
        }

        private AllDataTypesEntity WriteReadValidateUsingSessionBatch(Table<AllDataTypesEntity> table)
        {
            Batch batch = _session.CreateBatch();
            AllDataTypesEntity expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            string uniqueKey = expectedDataTypesEntityRow.StringType;
            batch.Append(table.Insert(expectedDataTypesEntityRow));
            batch.Execute();

            List<AllDataTypesEntity> listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            AllDataTypesEntity actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
            return expectedDataTypesEntityRow;
        }


        private AllDataTypesEntity WriteReadValidate(Table<AllDataTypesEntity> table)
        {
            WriteReadValidateUsingSessionBatch(table);
            return WriteReadValidateUsingTableMethods(table);
        }

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

        private class PrivateClassMissingPartitionKey
        {
            private string StringValue = "someStringValue";
        }



    }
}
