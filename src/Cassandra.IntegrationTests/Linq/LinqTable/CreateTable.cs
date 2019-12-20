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
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.SimulacronAPI.SystemTables;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using Newtonsoft.Json;
using NUnit.Framework;
#pragma warning disable 612
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    public class CreateTable : SimulacronTest
    {
        private const string CreateCqlColumns =
            "\"boolean_type\" boolean, " +
            "\"date_time_offset_type\" timestamp, " +
            "\"date_time_type\" timestamp, " +
            "\"decimal_type\" decimal, " +
            "\"double_type\" double, " +
            "\"float_type\" float, " +
            "\"guid_type\" uuid, " +
            "\"int_type\" int, " +
            "\"int64_type\" bigint, " +
            "\"list_of_guids_type\" list<uuid>, " +
            "\"list_of_strings_type\" list<text>, " +
            "\"map_type_string_long_type\" map<text, bigint>, " +
            "\"map_type_string_string_type\" map<text, text>, " +
            "\"nullable_date_time_type\" timestamp, " +
            "\"nullable_int_type\" int, " +
            "\"nullable_time_uuid_type\" timeuuid, " +
            "\"string_type\" text, " +
            "\"time_uuid_type\" timeuuid";

        private const string CreateCqlDefaultColumns =
            "BooleanType boolean, " +
            "DateTimeOffsetType timestamp, " +
            "DateTimeType timestamp, " +
            "DecimalType decimal, " +
            "DictionaryStringLongType map<text, bigint>, " +
            "DictionaryStringStringType map<text, text>, " +
            "DoubleType double, " +
            "FloatType float, " +
            "GuidType uuid, " +
            "Int64Type bigint, " +
            "IntType int, " +
            "ListOfGuidsType list<uuid>, " +
            "ListOfStringsType list<text>, " +
            "NullableDateTimeType timestamp, " +
            "NullableIntType int, " +
            "NullableTimeUuidType timeuuid, " +
            "StringType text, " +
            "TimeUuidType timeuuid";
        
        private const string CreateCqlDefaultColumnsCaseSensitive =
            "\"BooleanType\" boolean, " +
            "\"DateTimeOffsetType\" timestamp, " +
            "\"DateTimeType\" timestamp, " +
            "\"DecimalType\" decimal, " +
            "\"DictionaryStringLongType\" map<text, bigint>, " +
            "\"DictionaryStringStringType\" map<text, text>, " +
            "\"DoubleType\" double, " +
            "\"FloatType\" float, " +
            "\"GuidType\" uuid, " +
            "\"Int64Type\" bigint, " +
            "\"IntType\" int, " +
            "\"ListOfGuidsType\" list<uuid>, " +
            "\"ListOfStringsType\" list<text>, " +
            "\"NullableDateTimeType\" timestamp, " +
            "\"NullableIntType\" int, " +
            "\"NullableTimeUuidType\" timeuuid, " +
            "\"StringType\" text, " +
            "\"TimeUuidType\" timeuuid";

        private static readonly string CreateCql =
            $"CREATE TABLE \"{AllDataTypesEntity.TableName}\" (" +
                CreateTable.CreateCqlColumns + ", " +
                "PRIMARY KEY (\"string_type\", \"guid_type\"))";
        
        private static readonly string CreateCqlFormatStr =
            "CREATE TABLE {0} (" +
            CreateTable.CreateCqlColumns + ", " +
            "PRIMARY KEY (\"string_type\", \"guid_type\"))";

        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
        
        /// <summary>
        /// Create a table using the method CreateIfNotExists
        /// 
        /// @Jira CSHARP-42  https://datastax-oss.atlassian.net/browse/CSHARP-42
        ///  - Jira detail: CreateIfNotExists causes InvalidOperationException
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateIfNotExist()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            table.CreateIfNotExists();
            VerifyStatement(QueryType.Query, CreateTable.CreateCql, 1);
        }

        /// <summary>
        /// Successfully create a table using the method Create
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            table.Create();
            VerifyStatement(QueryType.Query, CreateTable.CreateCql, 1);
        }
        
        [Test, TestCassandraVersion(4, 0, Comparison.LessThan)]
        public void Should_CreateTable_WhenClusteringOrderAndCompactOptionsAreSet()
        {
            var config = new MappingConfiguration().Define(
                new Map<Tweet>()
                    .PartitionKey(a => a.TweetId)
                    .ClusteringKey(a => a.AuthorId, SortOrder.Descending)
                    .CompactStorage());
            var table = new Table<Tweet>(Session, config);
            table.Create();
            VerifyStatement(
                QueryType.Query, 
                "CREATE TABLE Tweet (AuthorId text, Body text, TweetId uuid, PRIMARY KEY (TweetId, AuthorId)) " +
                "WITH CLUSTERING ORDER BY (AuthorId DESC) AND COMPACT STORAGE", 
                1);
        }

        /// <summary>
        /// Successfully create a table using the method Create, 
        /// overriding the default table name given via the class' "name" meta-tag
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_Create_NameOverride()
        {
            var uniqueTableName = TestUtils.GetUniqueTableName();
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration(), uniqueTableName);
            Assert.AreEqual(uniqueTableName, table.Name);
            table.Create();
            VerifyStatement(
                QueryType.Query, 
                CreateTable.CreateCql.Replace($"\"{AllDataTypesEntity.TableName}\"", $"\"{uniqueTableName}\""), 
                1);
        }

        /// <summary>
        /// Attempt to create the same table using the method Create twice, validate expected failure message
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateTable_AlreadyExists()
        {
            var tableName = "tbl_already_exists_1";
            var config = new MappingConfiguration().Define(
                new Map<AllDataTypesEntity>().TableName(tableName).PartitionKey(a => a.TimeUuidType));
            var table = new Table<AllDataTypesEntity>(Session, config);

            table.Create();
            VerifyStatement(
                QueryType.Query, 
                $"CREATE TABLE {tableName} ({CreateTable.CreateCqlDefaultColumns}, PRIMARY KEY (TimeUuidType))", 
                1);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"CREATE TABLE {tableName} ({CreateTable.CreateCqlDefaultColumns}, PRIMARY KEY (TimeUuidType))")
                      .ThenAlreadyExists(_uniqueKsName, tableName));

            var ex = Assert.Throws<AlreadyExistsException>(() => table.Create());

            Assert.AreEqual(tableName, ex.Table);
            Assert.AreEqual(_uniqueKsName, ex.Keyspace);
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
            var allDataTypesTable = new Table<AllDataTypesEntity>(Session, mappingConfig1);
            allDataTypesTable.Create();

            VerifyStatement(
                QueryType.Query,
                $"CREATE TABLE \"{staticTableName}\" ({CreateTable.CreateCqlDefaultColumnsCaseSensitive}, PRIMARY KEY (\"StringType\"))",
                1);

            // Second creation attempt with same table name should fail

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"CREATE TABLE \"{staticTableName}\" (\"Director\" text, " +
                            "\"ExampleSet\" list<text>, \"MainActor\" text, " +
                            "\"MovieMaker\" text, \"Title\" text, \"Year\" int, " +
                            "PRIMARY KEY (\"Title\"))")
                      .ThenAlreadyExists(_uniqueKsName, staticTableName));
            var mappingConfig2 = new MappingConfiguration().Define(new Map<Movie>().TableName(staticTableName).CaseSensitive().PartitionKey(c => c.Title));
            var movieTable = new Table<Movie>(Session, mappingConfig2);
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
            var allDataTypesTable = new Table<AllDataTypesEntity>(Session, mappingConfig);
            allDataTypesTable.Create();
            
            VerifyStatement(
                QueryType.Query,
                $"CREATE TABLE \"{staticTableName}\" ({CreateTable.CreateCqlDefaultColumnsCaseSensitive}, PRIMARY KEY (\"StringType\"))",
                1);

            // Second creation attempt with same table name should fail
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"CREATE TABLE \"{staticTableName}\" (" +
                            "\"director\" text, \"list\" list<text>, \"mainGuy\" text, " +
                            "\"movie_maker\" text, \"unique_movie_title\" text, " +
                            "\"yearMade\" int, " +
                            "PRIMARY KEY ((\"unique_movie_title\", \"movie_maker\"), \"director\"))")
                      .ThenAlreadyExists(_uniqueKsName, staticTableName));
            var movieTable = new Table<Movie>(Session, new MappingConfiguration(), staticTableName);
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
            if (!TestClusterManager.SchemaManipulatingQueriesThrowInvalidQueryException())
            {
                TestCluster.PrimeFluent(
                    b => b.WhenQuery(string.Format(CreateTable.CreateCqlFormatStr, $"\"{uniqueKsName}\".\"{uniqueTableName}\""))
                          .ThenServerError(ServerError.ConfigError, "msg"));
            }
            else
            {
                TestCluster.PrimeFluent(
                    b => b.WhenQuery(string.Format(CreateTable.CreateCqlFormatStr, $"\"{uniqueKsName}\".\"{uniqueTableName}\""))
                          .ThenServerError(ServerError.Invalid, "msg"));
            }

            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration(), uniqueTableName, uniqueKsName);
            if(!TestClusterManager.SchemaManipulatingQueriesThrowInvalidQueryException())
            {
                Assert.Throws<InvalidConfigurationInQueryException>(() => table.Create());
            }
            else
            {
                Assert.Throws<InvalidQueryException>(() => table.Create());
            }
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
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(string.Format(CreateTable.CreateCqlFormatStr, $"\"{uniqueKsName}\".\"{uniqueTableName}\""))
                      .ThenServerError(ServerError.ConfigError, "msg"));

            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration(), uniqueTableName, uniqueKsName);
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
            var sharedTableName = typeof(AllDataTypesEntity).Name;
            var mappingConfig = new MappingConfiguration().Define(new Map<AllDataTypesEntity>()
                                                                  .TableName(sharedTableName).CaseSensitive().PartitionKey(c => c.StringType));
            var table1 = new Table<AllDataTypesEntity>(Session, mappingConfig);
            table1.Create();

            VerifyStatement(
                QueryType.Query,
                $"CREATE TABLE \"{sharedTableName}\" ({CreateTable.CreateCqlDefaultColumnsCaseSensitive}, PRIMARY KEY (\"StringType\"))",
                1);

            WriteReadValidate(table1);

            // Create second table with same name in new keyspace
            var newUniqueKsName = TestUtils.GetUniqueKeyspaceName();
            Session.CreateKeyspace(newUniqueKsName);

            VerifyStatement(
                QueryType.Query,
                $"CREATE KEYSPACE \"{newUniqueKsName}\" " +
                "WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : '1'}" +
                " AND durable_writes = true",
                1);

            Assert.AreNotEqual(_uniqueKsName, newUniqueKsName);
            var table2 = new Table<AllDataTypesEntity>(Session, mappingConfig, sharedTableName, newUniqueKsName);
            table2.Create();

            VerifyStatement(
                QueryType.Query,
                $"CREATE TABLE \"{newUniqueKsName}\".\"{sharedTableName}\" ({CreateTable.CreateCqlDefaultColumnsCaseSensitive}, PRIMARY KEY (\"StringType\"))",
                1);

            WriteReadValidate(table2);

            // also use ChangeKeyspace and validate client functionality
            Session.ChangeKeyspace(_uniqueKsName);
            VerifyStatement(QueryType.Query, $"USE \"{_uniqueKsName}\"", 1);

            WriteReadValidate(table1);

            Session.ChangeKeyspace(newUniqueKsName);
            VerifyStatement(QueryType.Query, $"USE \"{newUniqueKsName}\"", 1);

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
            var table = new Table<PrivateClassMissingPartitionKey>(Session, mappingConfig);

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
            var table = new Table<PrivateEmptyClass>(Session, mappingConfig);

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
            
            VerifyStatement(
                QueryType.Query,
                "CREATE TABLE tbl_frozen_tuple (Id1 uuid, t frozen<tuple<bigint, bigint, text>>, PRIMARY KEY (Id1))",
                1);
            
            PrimeSystemSchemaTables(
                _uniqueKsName, 
                "tbl_frozen_tuple",
                new []
                {
                    new StubTableColumn("Id1", StubColumnKind.PartitionKey, DataType.Uuid),
                    new StubTableColumn("t", StubColumnKind.Regular, DataType.Frozen(DataType.Tuple(DataType.BigInt, DataType.BigInt, DataType.Text)))
                });

            SessionCluster.RefreshSchema(_uniqueKsName, "tbl_frozen_tuple");

            var tableMeta = SessionCluster.Metadata.GetTable(_uniqueKsName, "tbl_frozen_tuple");
            Assert.NotNull(tableMeta);
            Assert.AreEqual(2, tableMeta.TableColumns.Length);
            var column = tableMeta.ColumnsByName["t"];
            Assert.AreEqual(ColumnTypeCode.Tuple, column.TypeCode);
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
            
            VerifyStatement(
                QueryType.Query,
                "CREATE TABLE tbl_with_counter_static (" +
                "counter_col1 counter static, counter_col2 counter, id1 uuid, id2 text, " +
                "PRIMARY KEY (id1, id2))",
                1);

            PrimeSystemSchemaTables(
                _uniqueKsName, 
                "tbl_with_counter_static",
                new []
                {
                    new StubTableColumn("counter_col1", StubColumnKind.Regular, DataType.Counter),
                    new StubTableColumn("counter_col2", StubColumnKind.Regular, DataType.Counter), 
                    new StubTableColumn("id1", StubColumnKind.PartitionKey, DataType.Uuid), 
                    new StubTableColumn("id2", StubColumnKind.ClusteringKey, DataType.Text)
                });

            SessionCluster.RefreshSchema(_uniqueKsName, "tbl_with_counter_static");

            var tableMeta = SessionCluster.Metadata.GetTable(_uniqueKsName, "tbl_with_counter_static");
            Assert.AreEqual(4, tableMeta.TableColumns.Length);
        }

        /// <summary>
        /// Successfully create a table using the null column name
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void TableCreate_CreateWithPropertyName()
        {
            var table = new Table<TestEmptyClusteringColumnName>(Session, MappingConfiguration.Global);
            table.CreateIfNotExists();
            
            VerifyStatement(
                QueryType.Query,
                "CREATE TABLE \"test_empty_clustering_column_name\" (" +
                    "\"cluster\" text, \"id\" int, \"value\" text, " +
                    "PRIMARY KEY (\"id\", \"cluster\"))",
                1);
        }

        ///////////////////////////////////////////////
        // Test Helpers
        //////////////////////////////////////////////

        // AllDataTypes

        private void WriteReadValidateUsingTableMethods(Table<AllDataTypesEntity> table)
        {
            TestCluster.PrimeDelete();
            var ksAndTable =
                table.KeyspaceName == null
                    ? $"\"{table.Name}\""
                    : $"\"{table.KeyspaceName}\".\"{table.Name}\"";

            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;

            // insert record
            Session.Execute(table.Insert(expectedDataTypesEntityRow));

            VerifyStatement(
                QueryType.Query,
                string.Format(
                    AllDataTypesEntity.InsertCqlDefaultColumnsFormatStr, 
                    ksAndTable),
                1,
                expectedDataTypesEntityRow.GetColumnValuesForDefaultColumns());

            // select record
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          string.Format(AllDataTypesEntity.SelectCqlDefaultColumnsFormatStr, ksAndTable),
                          p => p.WithParam(uniqueKey))
                      .ThenRowsSuccess(
                          AllDataTypesEntity.GetDefaultColumns(),
                          r => r.WithRow(expectedDataTypesEntityRow.GetColumnValuesForDefaultColumns())));


            var listOfAllDataTypesObjects = 
                (from x in table where x.StringType.Equals(uniqueKey) select x)
                .Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            var actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
        }

        private void WriteReadValidateUsingSessionBatch(Table<AllDataTypesEntity> table)
        {
            TestCluster.PrimeDelete();
            var ksAndTable = table.KeyspaceName == null
                ? $"\"{table.Name}\""
                : $"\"{table.KeyspaceName}\".\"{table.Name}\"";

            var batch = Session.CreateBatch();
            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;
            batch.Append(table.Insert(expectedDataTypesEntityRow));
            batch.Execute();

            VerifyBatchStatement(
                1,
                new [] { string.Format(
                    AllDataTypesEntity.InsertCqlDefaultColumnsFormatStr, 
                    ksAndTable) },
                new [] { expectedDataTypesEntityRow.GetColumnValuesForDefaultColumns() });

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          string.Format(AllDataTypesEntity.SelectCqlDefaultColumnsFormatStr, ksAndTable),
                          p => p.WithParam(uniqueKey))
                      .ThenRowsSuccess(
                          AllDataTypesEntity.GetDefaultColumns(),
                          r => r.WithRow(expectedDataTypesEntityRow.GetColumnValuesForDefaultColumns())));

            var listOfAllDataTypesObjects = 
                (from x in table 
                 where x.StringType.Equals(uniqueKey) 
                 select x)
                .Execute().ToList();
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
