using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dse.Test.Unit;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class SchemaAgreementSimulacronTests
    {
        private const int MaxSchemaAgreementWaitSeconds = 10;

        private const string LocalSchemaVersionQuery = "SELECT schema_version FROM system.local";
        private const string PeersSchemaVersionQuery = "SELECT schema_version FROM system.peers";

        private static IPrimeRequest LocalSchemaVersionQueryPrime(Guid version) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.LocalSchemaVersionQuery)
                .ThenRowsSuccess(new[] { ("schema_version", DataType.Uuid) }, rows => rows.WithRow(version))
                .BuildRequest();

        private static IPrimeRequest PeersSchemaVersionQueryPrime(IEnumerable<Guid> versions) =>
            SimulacronBase
                .PrimeBuilder()
                .WhenQuery(SchemaAgreementSimulacronTests.PeersSchemaVersionQuery)
                .ThenRowsSuccess(
                    new[] { ("schema_version", DataType.Uuid) },
                    rows => rows.WithRows(versions.Select(v => new object[] { v }).ToArray()))
                .BuildRequest();

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
            var queryPrime =
                SimulacronBase
                    .PrimeBuilder()
                    .WhenQuery(selectStatement)
                    .ThenRowsSuccess(new[] { ("test", DataType.Ascii) }, rows => rows.WithRow("123"))
                    .BuildRequest();

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