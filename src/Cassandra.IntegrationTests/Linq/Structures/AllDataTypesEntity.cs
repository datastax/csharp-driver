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
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.When;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.Structures
{
    [AllowFiltering]
    [Table(AllDataTypesEntity.TableName)]
    public class AllDataTypesEntity : IAllDataTypesEntity
    {
        public const string TableName = "allDataTypes";

        private static readonly IDictionary<string, Func<AllDataTypesEntity, object>> ColumnMappings =
            new Dictionary<string, Func<AllDataTypesEntity, object>>
            {
                { "boolean_type", entity => entity.BooleanType },
                { "date_time_offset_type", entity => entity.DateTimeOffsetType },
                { "date_time_type", entity => entity.DateTimeType },
                { "decimal_type", entity => entity.DecimalType },
                { "double_type", entity => entity.DoubleType },
                { "float_type", entity => entity.FloatType },
                { "guid_type", entity => entity.GuidType },
                { "int_type", entity => entity.IntType },
                { "int64_type", entity => entity.Int64Type },
                { "list_of_guids_type", entity => entity.ListOfGuidsType },
                { "list_of_strings_type", entity => entity.ListOfStringsType },
                { "map_type_string_long_type", entity => entity.DictionaryStringLongType },
                { "map_type_string_string_type", entity => entity.DictionaryStringStringType },
                { "nullable_date_time_type", entity => entity.NullableDateTimeType },
                { "nullable_int_type", entity => entity.NullableIntType },
                { "nullable_time_uuid_type", entity => entity.NullableTimeUuidType },
                { "string_type", entity => entity.StringType },
                { "time_uuid_type", entity => entity.TimeUuidType }
            };

        private static readonly IDictionary<string, DataType> ColumnnsToDataTypes =
            new Dictionary<string, DataType>
            {
                { "boolean_type", DataType.GetDataType(typeof(bool)) },
                { "date_time_offset_type", DataType.GetDataType(typeof(DateTimeOffset)) },
                { "date_time_type", DataType.GetDataType(typeof(DateTime)) },
                { "decimal_type", DataType.GetDataType(typeof(decimal)) },
                { "double_type", DataType.GetDataType(typeof(double)) },
                { "float_type", DataType.GetDataType(typeof(float)) },
                { "guid_type", DataType.GetDataType(typeof(Guid)) },
                { "int_type", DataType.GetDataType(typeof(int)) },
                { "int64_type", DataType.GetDataType(typeof(long)) },
                { "list_of_guids_type", DataType.GetDataType(typeof(List<Guid>)) },
                { "list_of_strings_type", DataType.GetDataType(typeof(List<string>)) },
                { "map_type_string_long_type", DataType.GetDataType(typeof(Dictionary<string, long>)) },
                { "map_type_string_string_type", DataType.GetDataType(typeof(Dictionary<string, string>)) },
                { "nullable_date_time_type", DataType.GetDataType(typeof(DateTime?)) },
                { "nullable_int_type", DataType.GetDataType(typeof(int?)) },
                { "nullable_time_uuid_type", DataType.GetDataType(typeof(TimeUuid?)) },
                { "string_type", DataType.GetDataType(typeof(string)) },
                { "time_uuid_type", DataType.GetDataType(typeof(TimeUuid)) }
            };

        public const int DefaultListLength = 5;

        [PartitionKey]
        [Column("string_type")]
        public string StringType { get; set; }

        [ClusteringKey(1)]
        [Column("guid_type")]
        public Guid GuidType { get; set; }

        [Column("date_time_type")]
        public DateTime DateTimeType { get; set; }

        [Column("nullable_date_time_type")]
        public DateTime? NullableDateTimeType { get; set; }

        [Column("date_time_offset_type")]
        public DateTimeOffset DateTimeOffsetType { get; set; }

        [Column("boolean_type")]
        public bool BooleanType { get; set; }

        [Column("decimal_type")]
        public Decimal DecimalType { get; set; }

        [Column("double_type")]
        public double DoubleType { get; set; }

        [Column("float_type")]
        public float FloatType { get; set; }

        [Column("nullable_int_type")]
        public int? NullableIntType { get; set; }

        [Column("int_type")]
        public int IntType { get; set; }

        [Column("int64_type")]
        public Int64 Int64Type { get; set; }

        [Column("time_uuid_type")]
        public TimeUuid TimeUuidType { get; set; }

        [Column("nullable_time_uuid_type")]
        public TimeUuid? NullableTimeUuidType { get; set; }

        [Column("map_type_string_long_type")]
        public Dictionary<string, long> DictionaryStringLongType { get; set; }

        [Column("map_type_string_string_type")]
        public Dictionary<string, string> DictionaryStringStringType { get; set; }

        [Column("list_of_guids_type")]
        public List<Guid> ListOfGuidsType { get; set; }

        [Column("list_of_strings_type")]
        public List<string> ListOfStringsType { get; set; }

        public static AllDataTypesEntity GetRandomInstance()
        {
            AllDataTypesEntity adte = new AllDataTypesEntity();
            return (AllDataTypesEntity)AllDataTypesEntityUtil.Randomize(adte);
        }

        public void AssertEquals(AllDataTypesEntity actualEntity)
        {
            AllDataTypesEntityUtil.AssertEquals(this, actualEntity);
        }

        public static List<AllDataTypesEntity> GetDefaultAllDataTypesList()
        {
            List<AllDataTypesEntity> objectList = new List<AllDataTypesEntity>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                objectList.Add(GetRandomInstance());
            }
            return objectList;
        }

        public static List<AllDataTypesEntity> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = new Table<AllDataTypesEntity>(session, new Cassandra.Mapping.MappingConfiguration());
            table.Create();

            List<AllDataTypesEntity> allDataTypesRandomList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static (string, DataType)[] GetColumnsWithTypes()
        {
            return AllDataTypesEntity.ColumnMappings.Keys.Zip(ColumnnsToDataTypes, (key, kvp) => (key, kvp.Value)).ToArray();
        }

        public static RowsResult GetEmptyRowsResult()
        {
            return new RowsResult(GetColumnsWithTypes());
        }

        public RowsResult CreateRowsResult()
        {
            return AddRow(AllDataTypesEntity.GetEmptyRowsResult());
        }

        private IWhenQueryFluent WithParams(IWhenQueryFluent when, params string[] columns)
        {
            return columns.Aggregate(when, (current, c) => current.WithParam(AllDataTypesEntity.ColumnMappings[c](this)));
        }

        public const string SelectCql = 
            "SELECT \"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
                "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\", \"int_type\", " +
                "\"int64_type\", \"list_of_guids_type\", \"list_of_strings_type\", \"map_type_string_long_type\"," +
                " \"map_type_string_string_type\", \"nullable_date_time_type\", \"nullable_int_type\", " +
                "\"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" FROM \"allDataTypes\" " +
            "WHERE \"string_type\" = ? " +
            "ALLOW FILTERING";

        public const string SelectRangeCql =
            "SELECT " +
            "\"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
            "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\"," +
            " \"int_type\", \"int64_type\", \"list_of_guids_type\", \"list_of_strings_type\"," +
            " \"map_type_string_long_type\", \"map_type_string_string_type\", \"nullable_date_time_type\"," +
            " \"nullable_int_type\", \"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" " +
            "FROM \"allDataTypes\" " +
            "ALLOW FILTERING";

        public void PrimeSelect(SimulacronCluster testCluster)
        {
            testCluster.PrimeFluent(b => When(testCluster, b).ThenRowsSuccess(this.CreateRowsResult()));
        }
        
        public IWhenFluent When(SimulacronCluster testCluster, IPrimeRequestFluent builder)
        {
            return builder.WhenQuery(
                          AllDataTypesEntity.SelectCql,
                          when => this.WithParams(when, "string_type"));
        }

        public const string InsertCql =
            "INSERT INTO \"allDataTypes\" (\"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
            "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\"," +
            " \"int_type\", \"int64_type\", \"list_of_guids_type\", \"list_of_strings_type\"," +
            " \"map_type_string_long_type\", \"map_type_string_string_type\", \"nullable_date_time_type\"," +
            " \"nullable_int_type\", \"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\") " +
            "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        public void PrimeQuery(SimulacronCluster testCluster, string cql, params string[] paramNames)
        {
            testCluster.PrimeFluent(
                b => b.WhenQuery(cql, when => this.WithParams(when, paramNames))
                      .ThenRowsSuccess(this.CreateRowsResult()));
        }
        
        public RowsResult AddRow(RowsResult result)
        {
            return (RowsResult) result.WithRow(GetColumnValues());
        }

        public object[] GetColumnValues()
        {
            return AllDataTypesEntity.ColumnMappings.Values.Select(func => func(this)).ToArray();
        }

        public static RowsResult AddRows(IEnumerable<AllDataTypesEntity> data)
        {
            return data.Aggregate(AllDataTypesEntity.GetEmptyRowsResult(), (current, c) => c.AddRow(current));
        }

        public static void PrimeCountQuery(SimulacronCluster testCluster, long count)
        {
            testCluster.PrimeFluent(
                b => b.WhenQuery("SELECT count(*) FROM \"allDataTypes\" ALLOW FILTERING")
                      .ThenRowsSuccess(new [] { "count" }, rows => rows.WithRow(count)));
        }

        public static void PrimeRangeSelect(SimulacronCluster testCluster, IEnumerable<AllDataTypesEntity> data)
        {
            testCluster.PrimeFluent(b => b.WhenQuery(AllDataTypesEntity.SelectRangeCql).ThenRowsSuccess(AddRows(data)));
        }
    }
}