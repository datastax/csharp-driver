namespace Cassandra.IntegrationTests.Core
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Cassandra.IntegrationTests.TestBase;
    using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

    using NUnit.Framework;

    [TestFixture, Category("short"), Category("schema_tests")]
    public class SchemaAgreementSimulacronTests
    {
        private const int MaxSchemaAgreementWaitSeconds = 10;

        private const string LocalSchemaVersionQuery = "SELECT schema_version FROM system.local";
        private const string PeersSchemaVersionQuery = "SELECT schema_version FROM system.peers";

        private static object LocalSchemaVersionQueryPrime(Guid version) => new
        {
            when = new { query = LocalSchemaVersionQuery },
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
            when = new { query = PeersSchemaVersionQuery },
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
                          .WithMaxSchemaAgreementWaitSeconds(MaxSchemaAgreementWaitSeconds)
                          .Build();
        }

        [Test]
        public async Task Should_CheckSchemaAgreementReturnTrue_When_OneSchemaVersionIsReturnedByQueries()
        {
            var schemaVersion = Guid.NewGuid();
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                simulacronCluster.Prime(LocalSchemaVersionQueryPrime(schemaVersion));
                simulacronCluster.Prime(PeersSchemaVersionQueryPrime(Enumerable.Repeat(schemaVersion, 2)));
                using (var cluster = BuildCluster(simulacronCluster))
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
                simulacronCluster.Prime(LocalSchemaVersionQueryPrime(schemaVersion1));
                simulacronCluster.Prime(PeersSchemaVersionQueryPrime(new[] { schemaVersion1, schemaVersion2 }));
                using (var cluster = BuildCluster(simulacronCluster))
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
                simulacronCluster.Prime(LocalSchemaVersionQueryPrime(schemaVersion1));
                simulacronCluster.Prime(PeersSchemaVersionQueryPrime(new[] { schemaVersion1, schemaVersion2 }));
                using (var cluster = BuildCluster(simulacronCluster))
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