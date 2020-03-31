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
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.SystemTables;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    [Category(TestCategory.Short)]
    public class EmptyColumnTests : TestGlobals
    {
        [Test]
        [TestCassandraVersion(3, 0, 0)]
        public void Should_ReturnCorrectValue_When_EmptyColumnNameAndSchemaParserV2()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.PrimeSystemSchemaTablesV2(
                    "testks",
                    "testtable",
                    new[] 
                    { 
                        new StubTableColumn("", StubColumnKind.PartitionKey, DataType.GetDataType(typeof(string))), 
                        new StubTableColumn(" ", StubColumnKind.ClusteringKey, DataType.GetDataType(typeof(string)))
                    });

                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery("SELECT \"\", \" \" FROM testks.testtable")
                          .ThenRowsSuccess(new[] { ("", DataType.Ascii), (" ", DataType.Ascii) }, rows => rows.WithRow("testval", "testval2")));

                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery(
                              "SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?",
                              query => query.WithParam(DataType.Ascii, "testval").WithParam(DataType.Ascii, "testval2"))
                          .ThenRowsSuccess(
                              new[] { ("", DataType.Ascii), (" ", DataType.Ascii) },
                              rows => rows.WithRow("testval", "testval2")));

                var mapConfig = new MappingConfiguration();
                mapConfig.Define(
                    new Map<TestTable>()
                        .KeyspaceName("testks")
                        .TableName("testtable")
                        .PartitionKey(u => u.TestColumn)
                        .Column(u => u.TestColumn, cm => cm.WithName(""))
                        .Column(u => u.TestColumn2, cm => cm.WithName(" ")));

                using (var cluster = EmptyColumnTests.BuildCluster(simulacronCluster))
                {
                    var session = cluster.Connect();

                    var testTables = new Table<TestTable>(session);
                    var test = (from t in testTables.Execute() select t).First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var mapper = new Mapper(session, mapConfig);
                    test = mapper.Fetch<TestTable>().First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var tableMetadata = session.Cluster.Metadata.GetTable("testks", "testtable");
                    Assert.IsNotNull(tableMetadata);

                    var rs = session.Execute("SELECT \"\", \" \" FROM testks.testtable");
                    AssertRowSetContainsCorrectValues(rs);

                    var ps = session.Prepare("SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?");
                    var bs = ps.Bind("testval", "testval2");
                    rs = session.Execute(bs);
                    AssertRowSetContainsCorrectValues(rs);
                }
            }
        }

        [Test]
        [TestCassandraVersion(3, 0, 0, Comparison.LessThan)]
        public void Should_ReturnCorrectValue_When_EmptyColumnNameAndSchemaParserV1()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.PrimeSystemSchemaTablesV1(
                    "testks",
                    "testtable",
                    new[] 
                    { 
                        new StubTableColumn("", StubColumnKind.PartitionKey, DataType.GetDataType(typeof(string))), 
                        new StubTableColumn(" ", StubColumnKind.ClusteringKey, DataType.GetDataType(typeof(string)))
                    });

                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery("SELECT \"\", \" \" FROM testks.testtable")
                          .ThenRowsSuccess(new[] { ("", DataType.Ascii), (" ", DataType.Ascii) }, rows => rows.WithRow("testval", "testval2")));

                simulacronCluster.PrimeFluent(
                    b => b.WhenQuery(
                              "SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?",
                              query => query.WithParam(DataType.Ascii, "testval").WithParam(DataType.Ascii, "testval2"))
                          .ThenRowsSuccess(
                              new[] { ("", DataType.Ascii), (" ", DataType.Ascii) },
                              rows => rows.WithRow("testval", "testval2")));

                var mapConfig = new MappingConfiguration();
                mapConfig.Define(
                    new Map<TestTable>()
                        .KeyspaceName("testks")
                        .TableName("testtable")
                        .PartitionKey(u => u.TestColumn)
                        .Column(u => u.TestColumn, cm => cm.WithName(""))
                        .Column(u => u.TestColumn2, cm => cm.WithName(" ")));

                using (var cluster = EmptyColumnTests.BuildCluster(simulacronCluster))
                {
                    var session = cluster.Connect();

                    var testTables = new Table<TestTable>(session);
                    var test = (from t in testTables.Execute() select t).First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var mapper = new Mapper(session, mapConfig);
                    test = mapper.Fetch<TestTable>().First();
                    Assert.AreEqual("testval", test.TestColumn);
                    Assert.AreEqual("testval2", test.TestColumn2);

                    var tableMetadata = session.Cluster.Metadata.GetTable("testks", "testtable");
                    Assert.IsNotNull(tableMetadata);

                    var rs = session.Execute("SELECT \"\", \" \" FROM testks.testtable");
                    AssertRowSetContainsCorrectValues(rs);

                    var ps = session.Prepare("SELECT \"\", \" \" FROM testks.testtable WHERE \"\" = ? AND \" \" = ?");
                    var bs = ps.Bind("testval", "testval2");
                    rs = session.Execute(bs);
                    AssertRowSetContainsCorrectValues(rs);
                }
            }
        }

        private void AssertRowSetContainsCorrectValues(RowSet rs)
        {
            var row = rs.Single();
            Assert.IsTrue(rs.Columns.Length == 2 && rs.Columns.Any(c => c.Name == string.Empty) && rs.Columns.Any(c => c.Name == " "));
            Assert.AreEqual("testval", row.GetValue<string>(string.Empty));
            Assert.AreEqual("testval2", row.GetValue<string>(" "));
        }

        private static Cluster BuildCluster(SimulacronCluster simulacronCluster)
        {
            return Cluster.Builder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .Build();
        }

        [Cassandra.Mapping.Attributes.Table(Name = "testtable", Keyspace = "testks")]
        private class TestTable
        {
            [Cassandra.Mapping.Attributes.Column("")]
            public string TestColumn { get; set; }

            [Cassandra.Mapping.Attributes.Column(" ")]
            public string TestColumn2 { get; set; }
        }
    }
}