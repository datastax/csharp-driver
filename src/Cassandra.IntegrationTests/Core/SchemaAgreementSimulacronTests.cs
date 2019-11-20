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
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class SchemaAgreementSimulacronTests
    {
        private const int MaxSchemaAgreementWaitSeconds = 10;

        private const string LocalSchemaVersionQuery = "SELECT schema_version FROM system.local";
        private const string PeersSchemaVersionQuery = "SELECT schema_version FROM system.peers";

        private static object LocalSchemaVersionQueryPrime(Guid version) => new
        {
            when = new { query = SchemaAgreementSimulacronTests.LocalSchemaVersionQuery },
            then = new
            {
                result = "success",
                delay_in_ms = 0,
                rows = new[] { new { schema_version = version } },
                column_types = new { schema_version = "uuid" },
                ignore_on_prepare = false
            }
        };

        private static object PeersSchemaVersionQueryPrime(IEnumerable<Guid> versions) => new
        {
            when = new { query = SchemaAgreementSimulacronTests.PeersSchemaVersionQuery },
            then = new
            {
                result = "success",
                delay_in_ms = 0,
                rows = versions.Select(v => new { schema_version = v }).ToArray(),
                column_types = new { schema_version = "uuid" },
                ignore_on_prepare = false
            }
        };

        private static Cluster BuildCluster(SimulacronCluster simulacronCluster)
        {
            return Cluster.Builder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .WithSocketOptions(
                              new SocketOptions()
                                  .SetReadTimeoutMillis(5000)
                                  .SetConnectTimeoutMillis(10000))
                          .WithMaxSchemaAgreementWaitSeconds(SchemaAgreementSimulacronTests.MaxSchemaAgreementWaitSeconds)
                          .Build();
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnTrue_When_OneSchemaVersionIsReturnedByQueries()
        {
            var schemaVersion = Guid.NewGuid();
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.LocalSchemaVersionQueryPrime(schemaVersion));
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.PeersSchemaVersionQueryPrime(Enumerable.Repeat(schemaVersion, 2)));
                using (var cluster = SchemaAgreementSimulacronTests.BuildCluster(simulacronCluster))
                {
                    cluster.Connect();
                    Assert.IsTrue(await cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
                }
            }
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnFalse_When_TwoSchemaVersionsAreReturnedByQueries()
        {
            var schemaVersion1 = Guid.NewGuid();
            var schemaVersion2 = Guid.NewGuid();
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.LocalSchemaVersionQueryPrime(schemaVersion1));
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.PeersSchemaVersionQueryPrime(new[] { schemaVersion1, schemaVersion2 }));
                using (var cluster = SchemaAgreementSimulacronTests.BuildCluster(simulacronCluster))
                {
                    cluster.Connect();
                    Assert.IsFalse(await cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
                }
            }
        }

        [Test]
        public async Task Should_SchemaInAgreementReturnTrue_When_ThereIsNoSchemaChangedResponseDespiteMultipleSchemaVersions()
        {
            var schemaVersion1 = Guid.NewGuid();
            var schemaVersion2 = Guid.NewGuid();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var selectStatement = $"SELECT test FROM k.{tableName}";
            var queryPrime = new
            {
                when = new { query = selectStatement },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[] { new { test = "123" } },
                    column_types = new { test = "ascii" },
                    ignore_on_prepare = false
                }
            };

            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.LocalSchemaVersionQueryPrime(schemaVersion1));
                simulacronCluster.Prime(SchemaAgreementSimulacronTests.PeersSchemaVersionQueryPrime(new[] { schemaVersion1, schemaVersion2 }));
                using (var cluster = SchemaAgreementSimulacronTests.BuildCluster(simulacronCluster))
                {
                    var session = cluster.Connect();

                    simulacronCluster.Prime(queryPrime);
                    var cql = new SimpleStatement(selectStatement);
                    var rowSet = await session.ExecuteAsync(cql).ConfigureAwait(false);
                    Assert.AreEqual("123", rowSet.First().GetValue<string>("test"));
                    Assert.IsTrue(rowSet.Info.IsSchemaInAgreement);
                    Assert.IsFalse(await cluster.Metadata.CheckSchemaAgreementAsync().ConfigureAwait(false));
                }
            }
        }
    }
}