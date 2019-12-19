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
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    public class CreateTable : SimulacronTest
    {
        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void CreateTable_FluentMapping_Success()
        {
            var mappingConfig = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(Session, mappingConfig);
            table.Create();

            VerifyQuery(
                "CREATE TABLE \"ManyDataTypesPoco\" " +
                $"({string.Join(", ", ManyDataTypesPoco.ColumnsToTypes.Select(k => $"\"{k.Key}\" {k.Value.Value}"))}, " +
                "PRIMARY KEY (\"StringType\"))",
                1);

            var mapper = new Mapper(Session, mappingConfig);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);

            VerifyBoundStatement(
                "INSERT INTO \"ManyDataTypesPoco\" (" +
                $"{string.Join(", ", ManyDataTypesPoco.ColumnsToTypes.Select(k => $"\"{k.Key}\""))})" +
                $" VALUES ({string.Join(", ", Enumerable.Range(0, ManyDataTypesPoco.ColumnsToTypes.Count).Select(_ => "?"))})",
                1,
                ManyDataTypesPoco.Columns.Values.Select(func => func(manyTypesInstance)).ToArray());

            var cqlSelect = $"SELECT * from \"{table.Name}\" where \"StringType\"='{manyTypesInstance.StringType}'";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            instancesQueried[0].AssertEquals(manyTypesInstance);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_PartitionKeyOmitted()
        {
            var mappingWithoutPk = new Map<ManyDataTypesPoco>();
            var table = new Table<ManyDataTypesPoco>(Session, new MappingConfiguration().Define(mappingWithoutPk));
            var expectedErrMsg = "Cannot create CREATE statement for POCO of type " + typeof(ManyDataTypesPoco).Name +
                                 " because it is missing PK columns id.  Are you missing a property/field on the POCO or did you forget to specify the PK columns in the mapping?";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "CREATE TABLE \"ManyDataTypesPoco\" " +
                          $"({string.Join(", ", ManyDataTypesPoco.ColumnsToTypes.Select(k => $"\"{k.Key}\" {k.Value.Value}"))})")
                      .ThenServerError(ServerError.Invalid, expectedErrMsg));

            var e = Assert.Throws<InvalidOperationException>(() => table.Create());
            Assert.AreEqual(expectedErrMsg, e.Message);
        }

        /// <summary>
        /// Attempt to insert a new record based on a mapping scheme that omits a partition key
        /// </summary>
        [Test]
        public void CreateTable_MakeAllPropertiesCaseSensitiveAtOnce()
        {
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(u => u.StringType)
                .TableName("tbl_case_sens_once")
                .CaseSensitive());

            var table = new Table<ManyDataTypesPoco>(Session, config);
            table.Create();

            VerifyQuery(
                "CREATE TABLE \"tbl_case_sens_once\" " +
                $"({string.Join(", ", ManyDataTypesPoco.ColumnsToTypes.Select(k => $"\"{k.Key}\" {k.Value.Value}"))}, " +
                "PRIMARY KEY (\"StringType\"))",
                1);

            var mapper = new Mapper(Session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);

            var cqlSelect = $"SELECT * from \"{table.Name}\" where \"StringType\"='{manyTypesInstance.StringType}'";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var objectsRetrieved = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            Assert.AreEqual(1, objectsRetrieved.Count);
            objectsRetrieved[0].AssertEquals(manyTypesInstance);
        }
    }
}