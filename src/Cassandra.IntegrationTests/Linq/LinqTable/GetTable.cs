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

using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Linq.LinqTable
{
    /// <summary>
    /// NOTE: The GetTable() method is deprecated.
    /// </summary>
    public class GetTable : SimulacronTest
    {
        private const string InsertCql =
            "INSERT INTO \"allDataTypes\" (\"boolean_type\", \"date_time_offset_type\", " +
                "\"date_time_type\", \"decimal_type\", \"double_type\", \"float_type\", " +
                "\"guid_type\", \"int64_type\", \"int_type\", \"list_of_guids_type\", " +
                "\"list_of_strings_type\", \"map_type_string_long_type\", \"map_type_string_string_type\", " +
                "\"nullable_date_time_type\", \"nullable_int_type\", \"nullable_time_uuid_type\", " +
                "\"string_type\", \"time_uuid_type\") " +
            "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        private const string SelectCql =
            "SELECT \"boolean_type\", \"date_time_offset_type\", \"date_time_type\", " +
                "\"decimal_type\", \"double_type\", \"float_type\", \"guid_type\", \"int64_type\", " +
                "\"int_type\", \"list_of_guids_type\", \"list_of_strings_type\", \"map_type_string_long_type\"," +
                " \"map_type_string_string_type\", \"nullable_date_time_type\", \"nullable_int_type\", " +
                "\"nullable_time_uuid_type\", \"string_type\", \"time_uuid_type\" FROM \"allDataTypes\" " +
            "WHERE \"string_type\" = ? " +
            "ALLOW FILTERING";

        /// <summary>
        /// Get table using GetTable, validate that the resultant table object functions as expected
        /// </summary>
        [Test]
        public void LinqTable_GetTable_Batch()
        {
            // Test
            var table = Session.GetTable<AllDataTypesEntity>();

            var batch = table.GetSession().CreateBatch();
            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;

            batch.Append(table.Insert(expectedDataTypesEntityRow));
            batch.Execute();

            VerifyBatchStatement(
                1,
                new[] { GetTable.InsertCql },
                new[] { expectedDataTypesEntityRow.GetColumnValues() });

            TestCluster.PrimeFluent(
                b => b.WhenQuery(GetTable.SelectCql, p => p.WithParam(uniqueKey))
                      .ThenRowsSuccess(expectedDataTypesEntityRow.CreateRowsResult()));

            var listOfAllDataTypesObjects =
                (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            var actualDataTypesEntityRow = listOfAllDataTypesObjects.First();
            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
        }

        [Test]
        public void LinqTable_GetTable_RegularStatement()
        {
            var table = Session.GetTable<AllDataTypesEntity>();
            var expectedDataTypesEntityRow = AllDataTypesEntity.GetRandomInstance();
            var uniqueKey = expectedDataTypesEntityRow.StringType;

            // insert record
            table.GetSession().Execute(table.Insert(expectedDataTypesEntityRow));

            VerifyStatement(QueryType.Query, GetTable.InsertCql, 1, expectedDataTypesEntityRow.GetColumnValues());

            TestCluster.PrimeDelete();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(GetTable.SelectCql, p => p.WithParam(uniqueKey))
                      .ThenRowsSuccess(expectedDataTypesEntityRow.CreateRowsResult()));

            // select record
            var listOfAllDataTypesObjects =
                (from x in table where x.StringType.Equals(uniqueKey) select x).Execute().ToList();
            Assert.NotNull(listOfAllDataTypesObjects);
            Assert.AreEqual(1, listOfAllDataTypesObjects.Count);
            var actualDataTypesEntityRow = listOfAllDataTypesObjects.First();

            expectedDataTypesEntityRow.AssertEquals(actualDataTypesEntityRow);
        }
    }
}