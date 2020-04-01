//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Linq;
using Dse.Test.Unit;
using Dse.Test.Integration.SimulacronAPI;
using Dse.Test.Integration.SimulacronAPI.Models;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [Category(TestCategory.Short), TestFixture]
    public class SimulacronHealthTest
    {
        [Test]
        public void Should_CreateSimulacronCluster()
        {
            const string query = "SELECT * FROM system.traces";
            var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" });
            var contactPoint = simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint);
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect();

                simulacronCluster.PrimeFluent(b =>
                    b.WhenQuery(query)
                     .ThenRowsSuccess(
                         new[] { ("id", DataType.Uuid), ("value", DataType.Varchar) },
                         rows => rows.WithRow(Guid.NewGuid(), "value")));

                var result = session.Execute(query);
                var firstRow = result.FirstOrDefault();
                Assert.NotNull(firstRow);
                Assert.AreEqual("value", firstRow["value"]);

                var logs = simulacronCluster.GetLogs();
                var dcLogs = logs.DataCenters;
                Assert.NotNull(dcLogs);
                Assert.True(logs.HasQueryBeenExecuted(query));
            }
        }
    }
}