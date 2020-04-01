//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.Then;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder.When;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using Guid = System.Guid;

#pragma warning disable 618

namespace Dse.Test.Integration.Linq.Structures
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
                { "int64_type", entity => entity.Int64Type },
                { "int_type", entity => entity.IntType },
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
            for (int i = 0; i < AllDataTypesEntity.DefaultListLength; i++)
            {
                objectList.Add(AllDataTypesEntity.GetRandomInstance());
            }
            return objectList;
        }

        public static List<AllDataTypesEntity> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = new Table<AllDataTypesEntity>(session, new Dse.Mapping.MappingConfiguration());
            table.Create();

            List<AllDataTypesEntity> allDataTypesRandomList = AllDataTypesEntity.GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static (string, DataType)[] GetColumnsWithTypes()
        {
            return AllDataTypesEntity.ColumnMappings.Keys.Zip(AllDataTypesEntity.ColumnnsToDataTypes, (key, kvp) => (key, kvp.Value)).ToArray();
        }

        public static RowsResult GetEmptyRowsResult()
        {
            return new RowsResult(AllDataTypesEntity.GetColumnsWithTypes());
        }

        public RowsResult CreateRowsResult()
        {
            return AddRow(AllDataTypesEntity.GetEmptyRowsResult());
        }

        private IWhenQueryBuilder WithParams(IWhenQueryBuilder when, params string[] columns)
        {
            return columns.Aggregate(when, (current, c) => current.WithParam(AllDataTypesEntity.ColumnMappings[c](this)));
        }

        public const string SelectCql = 
            "SELECT \"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
                "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\", \"int64_type\", " +
                "\"int_type\", \"list_of_guids_type\", \"list_of_strings_type\", \"map_type_string_long_type\"," +
                " \"map_type_string_string_type\", \"nullable_date_time_type\", \"nullable_int_type\", " +
                "\"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" FROM \"allDataTypes\" " +
            "WHERE \"string_type\" = ? " +
            "ALLOW FILTERING";
        
        public const string SelectCqlDefaultColumnsFormatStr = 
            "SELECT \"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
            "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", " +
            "\"FloatType\", \"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", " +
            "\"ListOfStringsType\", \"NullableDateTimeType\", \"NullableIntType\", " +
            "\"NullableTimeUuidType\", \"StringType\", \"TimeUuidType\" FROM {0} " +
            "WHERE \"StringType\" = ?";

        public const string SelectRangeCql =
            "SELECT " +
            "\"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
            "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\"," +
            " \"int64_type\", \"int_type\", \"list_of_guids_type\", \"list_of_strings_type\"," +
            " \"map_type_string_long_type\", \"map_type_string_string_type\", \"nullable_date_time_type\"," +
            " \"nullable_int_type\", \"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" " +
            "FROM \"allDataTypes\" " +
            "ALLOW FILTERING";

        public void PrimeSelect(SimulacronCluster testCluster)
        {
            testCluster.PrimeFluent(b => When(testCluster, b).ThenRowsSuccess(CreateRowsResult()));
        }
        
        public IWhenFluent When(SimulacronCluster testCluster, IPrimeRequestBuilder builder)
        {
            return builder.WhenQuery(
                          AllDataTypesEntity.SelectCql,
                          when => WithParams(when, "string_type"));
        }
        
        public const string InsertCqlDefaultColumnsFormatStr =
            "INSERT INTO {0} (" +
                "\"BooleanType\", \"DateTimeOffsetType\", \"DateTimeType\", \"DecimalType\", " +
                "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"DoubleType\", " +
                "\"FloatType\", \"GuidType\", \"Int64Type\", \"IntType\", \"ListOfGuidsType\", " +
                "\"ListOfStringsType\", \"NullableDateTimeType\", \"NullableIntType\", " +
                "\"NullableTimeUuidType\", \"StringType\", \"TimeUuidType\") " +
            "VALUES (" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        public const string InsertCqlFormatStr =
            "INSERT INTO {0} (\"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
            "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\"," +
            " \"int_type\", \"int64_type\", \"list_of_guids_type\", \"list_of_strings_type\"," +
            " \"map_type_string_long_type\", \"map_type_string_string_type\", \"nullable_date_time_type\"," +
            " \"nullable_int_type\", \"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\") " +
            "VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                b => b.WhenQuery(cql, when => WithParams(when, paramNames))
                      .ThenRowsSuccess(CreateRowsResult()));
        }
        
        public RowsResult AddRow(RowsResult result)
        {
            return (RowsResult) result.WithRow(GetColumnValues());
        }

        public object[] GetColumnValues()
        {
            return AllDataTypesEntity.ColumnMappings.Values.Select(func => func(this)).ToArray();
        }

        public static (string, DataType)[] GetDefaultColumns()
        {
            return new []
            {
                (nameof(AllDataTypesEntity.BooleanType), DataType.GetDataType(typeof(bool))),
                (nameof(AllDataTypesEntity.DateTimeOffsetType), DataType.GetDataType(typeof(DateTimeOffset))),
                (nameof(AllDataTypesEntity.DateTimeType), DataType.GetDataType(typeof(DateTime))),
                (nameof(AllDataTypesEntity.DecimalType), DataType.GetDataType(typeof(decimal))),
                (nameof(AllDataTypesEntity.DictionaryStringLongType), DataType.GetDataType(typeof(Dictionary<string, long>))),
                (nameof(AllDataTypesEntity.DictionaryStringStringType), DataType.GetDataType(typeof(Dictionary<string, string>))),
                (nameof(AllDataTypesEntity.DoubleType), DataType.GetDataType(typeof(double))),
                (nameof(AllDataTypesEntity.FloatType), DataType.GetDataType(typeof(float))),
                (nameof(AllDataTypesEntity.GuidType), DataType.GetDataType(typeof(Guid))),
                (nameof(AllDataTypesEntity.Int64Type), DataType.GetDataType(typeof(long))),
                (nameof(AllDataTypesEntity.IntType), DataType.GetDataType(typeof(int))),
                (nameof(AllDataTypesEntity.ListOfGuidsType), DataType.GetDataType(typeof(List<Guid>))),
                (nameof(AllDataTypesEntity.ListOfStringsType), DataType.GetDataType(typeof(List<string>))),
                (nameof(AllDataTypesEntity.NullableDateTimeType), DataType.GetDataType(typeof(DateTime?))),
                (nameof(AllDataTypesEntity.NullableIntType), DataType.GetDataType(typeof(int?))),
                (nameof(AllDataTypesEntity.NullableTimeUuidType), DataType.GetDataType(typeof(TimeUuid?))),
                (nameof(AllDataTypesEntity.StringType), DataType.GetDataType(typeof(string))),
                (nameof(AllDataTypesEntity.TimeUuidType), DataType.GetDataType(typeof(TimeUuid)))
            };
        }
        
        public object[] GetColumnValuesForDefaultColumns()
        {
            return new object[]
            {
                BooleanType,
                DateTimeOffsetType,
                DateTimeType,
                DecimalType,
                DictionaryStringLongType,
                DictionaryStringStringType,
                DoubleType,
                FloatType,
                GuidType,
                Int64Type,
                IntType,
                ListOfGuidsType,
                ListOfStringsType,
                NullableDateTimeType,
                NullableIntType,
                NullableTimeUuidType,
                StringType,
                TimeUuidType
            };
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
            testCluster.PrimeFluent(b => b.WhenQuery(AllDataTypesEntity.SelectRangeCql).ThenRowsSuccess(AllDataTypesEntity.AddRows(data)));
        }
    }
}