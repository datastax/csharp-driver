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
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
#pragma warning disable 612
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.LinqTable
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
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Create a table using the method CreateIfNotExists
        /// 
        /// @Jira CSHARP-42  https://datastax-oss.atlassian.net/browse/CSHARP-42
        ///  - Jira detail: CreateIfNotExists causes InvalidOperationException
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateIfNotExist()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.CreateIfNotExists();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            table.Create();
            WriteReadValidate(table);
        }

        /// <summary>
        /// Successfully create a table using the method Create, 
        /// overriding the default table name given via the class' "name" meta-tag
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create_NameOverride()
        {
            var uniqueTableName = TestUtils.GetUniqueTableName();
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration(), uniqueTableName);
            Assert.AreEqual(uniqueTableName, table.Name);
            table.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, uniqueTableName, true));
            WriteReadValidate(table);
        }

        /// <summary>
        /// Attempt to create the same table using the method Create twice, validate expected failure message
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateTable_AlreadyExists()
        {
            var config = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName("tbl_already_exists_1").PartitionKey(a => a.TimeUuidType));
            var table = new Table<AllDataTypesEntity>(_session, config);
            table.Create();
            Assert.Throws<AlreadyExistsException>(() => table.Create());
        }

        /// <summary>
        /// Attempt to create two tables of different types but with the same name using the Create method. 
        /// Validate expected failure message
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateTable_SameNameDifferentTypeAlreadyExists()
        {
            // First table name creation works as expected
            const string staticTableName = "staticTableName_1";
            var mappingConfig1 = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(staticTableName).CaseSensitive().PartitionKey(c => c.StringType));
            var allDataTypesTable = new Table<AllDataTypesEntity>(_session, mappingConfig1);
            allDataTypesTable.Create();

            // Second creation attempt with same table name should fail
            var mappingConfig2 = new MappingConfiguration().Define(new Map<Movie>().TableName(staticTableName).CaseSensitive().PartitionKey(c => c.Title));
            var movieTable = new Table<Movie>(_session, mappingConfig2);
            Assert.Throws<AlreadyExistsException>(() => movieTable.Create());
        }

        /// <summary>
        /// Attempt to create two tables of different types but with the same name using the Create method,
        /// setting table name of second create request via table name override option in constructor
        /// Validate expected failure message
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateTable_SameNameDifferentTypeAlreadyExists_TableNameOverride()
        {
            // First table name creation works as expected
            const string staticTableName = "staticTableName_2";
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(staticTableName).CaseSensitive().PartitionKey(c => c.StringType));
            var allDataTypesTable = new Table<AllDataTypesEntity>(_session, mappingConfig);
            allDataTypesTable.Create();

            // Second creation attempt with same table name should fail
            var movieTable = new Table<Movie>(_session, new MappingConfiguration(), staticTableName);
            Assert.Throws<AlreadyExistsException>(() => movieTable.Create());
        }

        /// <summary>
        /// Attempt to create a table in a non-existent keyspace, specifying the keyspace name in Table constructor's override option
        /// Validate error message.
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create_KeyspaceOverride_NoSuchKeyspace()
        {
            var uniqueTableName = TestUtils.GetUniqueTableName();
            var uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration(), uniqueTableName, uniqueKsName);
            Assert.Throws<InvalidConfigurationInQueryException>(() => table.Create());
        }

        /// <summary>
        /// Attempt to create a table in a non-existent keyspace, specifying the keyspace name in Table constructor's override option
        /// Validate error message.
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateIfNotExists_KeyspaceOverride_NoSuchKeyspace()
        {
            var uniqueTableName = TestUtils.GetUniqueTableName();
            var uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration(), uniqueTableName, uniqueKsName);
            Assert.Throws<InvalidConfigurationInQueryException>(() => table.CreateIfNotExists());
        }

        /// <summary>
        /// Successfully create two tables with the same name in two different keyspaces using the method Create
        /// Do not manually change the session to use the different keyspace
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create_TwoTablesSameName_TwoKeyspacesDifferentNames_KeyspaceOverride()
        {
            // Setup first table
            var sharedTableName = typeof (AllDataTypesEntity).Name;
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>().TableName(sharedTableName).CaseSensitive().PartitionKey(c => c.StringType));
            var table1 = new Table<AllDataTypesEntity>(_session, mappingConfig);
            table1.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, sharedTableName, true)); 
            WriteReadValidate(table1);

            // Create second table with same name in new keyspace
            var newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(newUniqueKsName);
            Assert.AreNotEqual(_uniqueKsName, newUniqueKsName);
            var table2 = new Table<AllDataTypesEntity>(_session, mappingConfig, sharedTableName, newUniqueKsName);
            table2.Create();
            Assert.IsTrue(TestUtils.TableExists(_session, newUniqueKsName, sharedTableName, true)); 
            WriteReadValidate(table2);

            // also use ChangeKeyspace and validate client functionality
            _session.ChangeKeyspace(_uniqueKsName);
            WriteReadValidate(table1);
            _session.ChangeKeyspace(newUniqueKsName);
            WriteReadValidate(table2);
        }

        /// <summary>
        /// Table creation fails because the referenced class is missing a partition key
        /// This also validates that a private class can be used with the Table.Create() method.
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_ClassMissingPartitionKey()
        {
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(PrivateClassMissingPartitionKey),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(PrivateClassMissingPartitionKey)));
            var table = new Table<PrivateClassMissingPartitionKey>(_session, mappingConfig);

            try
            {
                table.Create();
            }
            catch (InvalidOperationException e)
            {
                Assert.AreEqual("No partition key defined", e.Message);
            }
        }

        /// <summary>
        /// Table creation fails because the referenced class is missing a partition key
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_ClassEmpty()
        {
            var mappingConfig = new MappingConfiguration();
            mappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(PrivateEmptyClass),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(PrivateEmptyClass)));
            var table = new Table<PrivateEmptyClass>(_session, mappingConfig);

            try
            {
                table.Create();
            }
            catch (InvalidOperationException e)
            {
                Assert.AreEqual("No partition key defined", e.Message);
            }
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Tuple()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.Tuple1, cm => cm.WithName("t").AsFrozen())
                .TableName("tbl_frozen_tuple")
                .ExplicitColumns());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(_uniqueKsName, "tbl_frozen_tuple");
            Assert.NotNull(tableMeta);
            Assert.AreEqual(2, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["t"];
            Assert.AreEqual(ColumnTypeCode.Tuple, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Udt()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.Udt1, cm => cm.WithName("u").AsFrozen())
                .TableName("tbl_frozen_udt")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(_uniqueKsName, "tbl_frozen_udt");
            Assert.AreEqual(2, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["u"];
            Assert.AreEqual(ColumnTypeCode.Udt, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Key()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.UdtSet1, cm => cm.WithFrozenKey().WithName("s"))
                .Column(p => p.TupleMapKey1, cm => cm.WithFrozenKey().WithName("m"))
                .TableName("tbl_frozen_key")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(_uniqueKsName, "tbl_frozen_key");
            Assert.AreEqual(3, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["s"];
            Assert.AreEqual(ColumnTypeCode.Set, column.TypeCode);
            column = tableMeta.ColumnsByName["m"];
            Assert.AreEqual(ColumnTypeCode.Map, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Value()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.ListMapValue1, cm => cm.WithFrozenValue().WithName("m"))
                .Column(p => p.UdtList1, cm => cm.WithFrozenValue().WithName("l"))
                .TableName("tbl_frozen_value")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(_uniqueKsName, "tbl_frozen_value");
            Assert.AreEqual(3, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["l"];
            Assert.AreEqual(ColumnTypeCode.List, column.TypeCode);
            column = tableMeta.ColumnsByName["m"];
            Assert.AreEqual(ColumnTypeCode.Map, column.TypeCode);
        }

        [Test, TestCassandraVersion(2, 0, 7)]
        public void CreateTable_With_Counter_Static()
        {
            var config = new MappingConfiguration()
                .Define(new Map<AllTypesEntity>().ExplicitColumns()
                                                 .TableName("tbl_with_counter_static")
                                                 .PartitionKey(t => t.UuidValue)
                                                 .ClusteringKey(t => t.StringValue)
                                                 .Column(t => t.UuidValue, cm => cm.WithName("id1"))
                                                 .Column(t => t.StringValue, cm => cm.WithName("id2"))
                                                 .Column(t => t.Int64Value, cm => cm.WithName("counter_col1")
                                                                                    .AsCounter().AsStatic())
                                                 .Column(t => t.IntValue, cm => cm.WithName("counter_col2")
                                                                                  .AsCounter()));
            var table = new Table<AllTypesEntity>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(_uniqueKsName, "tbl_with_counter_static");
            Assert.AreEqual(4, tableMeta.TableColumns.Length);
        }

        /// <summary>
        /// Successfully create a table using the null column name
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateWithPropertyName()
        {
            var table = new Table<TestEmptyClusteringColumnName>(_session, MappingConfiguration.Global);
            table.CreateIfNotExists();
            _session.Execute(new SimpleStatement("insert into test_empty_clustering_column_name (id, cluster, value) " +
                                                 "values (1, 'c1','v1')"));
        }

        ///////////////////////////////////////////////
        // Test Helpers
        //////////////////////////////////////////////

        // AllDataTypes

        private void WriteReadValidateUsingTableMethods(Table<AllDataTypesEntity> table)
        {
            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;

            // insert record
            _session.Execute(table.Insert(expectedDataTypesEntityRow));

            // select record
            var listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            var actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
        }

        private void WriteReadValidateUsingSessionBatch(Table<AllDataTypesEntity> table)
        {
            var batch = _session.CreateBatch();
            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;
            batch.Append(table.Insert(expectedDataTypesEntityRow));
            batch.Execute();

            var listOfAllDataTypesObjects = (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            var actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
        }


        private void WriteReadValidate(Table<AllDataTypesEntity> table)
        {
            WriteReadValidateUsingSessionBatch(table);
            WriteReadValidateUsingTableMethods(table);
        }

        private class PrivateClassMissingPartitionKey
        {
            //Is never used, but don't mind
            #pragma warning disable 414, 169
            // ReSharper disable once InconsistentNaming
            private string StringValue = "someStringValue";
            #pragma warning restore 414, 169
        }

        private class PrivateEmptyClass
        {
        }

        [Table("test_empty_clustering_column_name")]
        // ReSharper disable once ClassNeverInstantiated.Local
        private class TestEmptyClusteringColumnName
        {
            [PartitionKey]
            [Column("id")]
            // ReSharper disable once UnusedMember.Local
            public int Id { get; set; }

            [ClusteringKey(1)]
            [Column]
            // ReSharper disable once InconsistentNaming
            // ReSharper disable once UnusedMember.Local
            public string cluster { get; set; }
            
            [Column]
            // ReSharper disable once InconsistentNaming
            // ReSharper disable once UnusedMember.Local
            public string value { get; set; }
        }
    }
}
