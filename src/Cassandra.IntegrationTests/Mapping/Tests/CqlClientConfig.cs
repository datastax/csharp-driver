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

using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    public class CqlClientConfig : SimulacronTest
    {
        private static readonly string CreateCqlCaseSensitive =
            "CREATE TABLE \"ManyDataTypesPoco\" (" +
                $"{string.Join(", ", ManyDataTypesPoco.GetColumnsAndTypesForCreate().Select(kvp => $"\"{kvp.Item1}\" {kvp.Item2.Value}"))}, " +
                "PRIMARY KEY (\"StringType\"))";

        private static readonly string CreateCql =
            "CREATE TABLE ManyDataTypesPoco (" +
            $"{string.Join(", ", ManyDataTypesPoco.GetColumnsAndTypesForCreate().Select(kvp => $"{kvp.Item1} {kvp.Item2.Value}"))}, " +
            "PRIMARY KEY (StringType))";

        /// <summary>
        /// Successfully insert and retrieve a Poco object that was created with fluent mapping,
        /// using a statically defined mapping class
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappingGeneric_StaticMappingClass()
        {
            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(Session, config);
            Assert.AreNotEqual(table.Name, table.Name.ToLower()); // make sure the case sensitivity rule is being used
            table.CreateIfNotExists();

            VerifyQuery(CreateCqlCaseSensitive, 1);

            var mapper = new Mapper(Session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);

            VerifyBoundStatement(
                $"INSERT INTO \"{table.Name}\" ({ManyDataTypesPoco.GetCaseSensitiveColumnNamesStr()}) " +
                $"VALUES ({string.Join(", ", ManyDataTypesPoco.GetColumnNames().Select(_ => "?"))})",
                1,
                manyTypesInstance.GetParameters());

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"SELECT {ManyDataTypesPoco.GetCaseSensitiveColumnNamesStr()} FROM \"{table.Name}\"")
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>().ToList();
            Assert.AreEqual(instancesQueried.Count, 1);
            manyTypesInstance.AssertEquals(instancesQueried[0]);
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object that was created with fluent mapping defined at run time,
        /// using UseIndividualMapping method that that uses general Poco type
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappingClassType_StaticMappingClass()
        {
            var config = new MappingConfiguration().Define(new ManyDataTypesPocoMappingCaseSensitive());
            var table = new Table<ManyDataTypesPoco>(Session, config);
            table.CreateIfNotExists();

            VerifyQuery(CreateCqlCaseSensitive, 1);

            var mapper = new Mapper(Session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);

            VerifyBoundStatement(
                $"INSERT INTO \"{table.Name}\" ({ManyDataTypesPoco.GetCaseSensitiveColumnNamesStr()}) " +
                $"VALUES ({string.Join(", ", ManyDataTypesPoco.GetColumnNames().Select(_ => "?"))})",
                1,
                manyTypesInstance.GetParameters());

            var cqlSelect = $"SELECT * from \"{table.Name}\" where \"StringType\"='{manyTypesInstance.StringType}'";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            ManyDataTypesPoco.AssertListEqualsList(new List<ManyDataTypesPoco> { manyTypesInstance }, instancesQueried);
        }

        /// <summary>
        /// Successfully insert and retrieve a Poco object using the method UseIndividualMappings()
        /// that uses a fluent mapping rule that was created during runtime
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappings_MappingDefinedDuringRuntime()
        {
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(c => c.StringType)
                .CaseSensitive());
            var table = new Table<ManyDataTypesPoco>(Session, config);
            table.CreateIfNotExists();

            VerifyQuery(CreateCqlCaseSensitive, 1);

            var mapper = new Mapper(Session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();

            mapper.Insert(manyTypesInstance);

            VerifyBoundStatement(
                $"INSERT INTO \"{table.Name}\" ({ManyDataTypesPoco.GetCaseSensitiveColumnNamesStr()}) " +
                $"VALUES ({string.Join(", ", ManyDataTypesPoco.GetColumnNames().Select(_ => "?"))})",
                1,
                manyTypesInstance.GetParameters());

            var cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name, "StringType", manyTypesInstance.StringType);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            ManyDataTypesPoco.AssertListEqualsList(new List<ManyDataTypesPoco> { manyTypesInstance }, instancesQueried);
        }

        /// <summary>
        /// Successfully insert a Poco instance
        /// </summary>
        [Test]
        public void CqlClientConfiguration_UseIndividualMappings_EmptyTypeDefinitionList()
        {
            // Setup
            var config = new MappingConfiguration().Define(new Map<ManyDataTypesPoco>()
                .PartitionKey(c => c.StringType));
            var table = new Table<ManyDataTypesPoco>(Session, config);
            table.CreateIfNotExists();

            VerifyQuery(CreateCql, 1);

            // validate default lower-casing
            Assert.AreNotEqual(typeof(ManyDataTypesPoco).Name.ToLower(), typeof(ManyDataTypesPoco).Name);
            Assert.AreNotEqual(table.Name.ToLower(), table.Name);
            Assert.AreEqual(typeof(ManyDataTypesPoco).Name.ToLower(), table.Name.ToLower());

            // Test
            var mapper = new Mapper(Session, config);
            var manyTypesInstance = ManyDataTypesPoco.GetRandomInstance();
            mapper.Insert(manyTypesInstance);

            VerifyBoundStatement(
                $"INSERT INTO {table.Name} ({ManyDataTypesPoco.GetColumnNamesStr()}) " +
                $"VALUES ({string.Join(", ", ManyDataTypesPoco.GetColumnNames().Select(_ => "?"))})",
                1,
                manyTypesInstance.GetParameters());

            // Verify results
            var cqlSelect = string.Format("SELECT * from \"{0}\" where \"{1}\"='{2}'", table.Name.ToLower(), "stringtype", manyTypesInstance.StringType);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(ManyDataTypesPoco.GetColumnsAndTypes(), r => r.WithRow(manyTypesInstance.GetParameters())));

            var instancesQueried = mapper.Fetch<ManyDataTypesPoco>(cqlSelect).ToList();
            ManyDataTypesPoco.AssertListEqualsList(new List<ManyDataTypesPoco> { manyTypesInstance }, instancesQueried);
        }
    }
}