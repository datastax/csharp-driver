//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Test.Integration.Policies.Util;
using Dse.Test.Integration.TestAttributes;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Unit;

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Dse.Test.Integration.Cloud
{
    [SniEnabledOnly]
    [TestFixture, Category("short")]
    public class CloudIntegrationTests : SharedCloudClusterTest
    {
        [Test]
        public async Task Should_HaveTwoRows_When_QueryingSystemPeers()
        {
            var rs = await Session.ExecuteAsync(new SimpleStatement("select * from system.peers")).ConfigureAwait(false);
            var allRs = rs.ToList();
            Assert.AreEqual(2, allRs.Count);
        }

        [Test]
        public void TokenAware_Prepared_Composite_NoHops()
        {
            // Setup
            var policyTestTools = new PolicyTestTools();

            // Test
            policyTestTools.CreateSchema(Session);
            policyTestTools.TableName = TestUtils.GetUniqueTableName();
            Session.Execute($"CREATE TABLE {policyTestTools.TableName} (k1 text, k2 int, i int, PRIMARY KEY ((k1, k2)))");
            Thread.Sleep(1000);
            var ps = Session.Prepare($"INSERT INTO {policyTestTools.TableName} (k1, k2, i) VALUES (?, ?, ?)");
            var traces = new List<QueryTrace>();
            for (var i = 0; i < 10; i++)
            {
                var statement = ps.Bind(i.ToString(), i, i).EnableTracing();
                //Routing key is calculated by the driver
                Assert.NotNull(statement.RoutingKey);
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there weren't any hops
            foreach (var t in traces)
            {
                //The coordinator must be the only one executing the query
                Assert.True(t.Events.All(e => e.Source.ToString() == t.Coordinator.ToString()), "There were trace events from another host for coordinator " + t.Coordinator);
            }
        }

        [Test]
        public void TokenAware_TargetWrongPartition_HopsOccur()
        {
            // Setup
            var policyTestTools = new PolicyTestTools { TableName = TestUtils.GetUniqueTableName() };

            policyTestTools.CreateSchema(Session, 1);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                                .SetRoutingKey(new RoutingKey() { RawRoutingKey = new byte[] { 0, 0, 0, 0 } })
                                .EnableTracing();
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
        }

        [Test]
        public void TokenAware_NoKey_HopsOccurAndAllNodesAreChosenAsCoordinators()
        {
            // Setup
            var policyTestTools = new PolicyTestTools { TableName = TestUtils.GetUniqueTableName() };

            policyTestTools.CreateSchema(Session, 1);
            var traces = new List<QueryTrace>();
            for (var i = 1; i < 10; i++)
            {
                //The partition key is wrongly calculated
                var statement = new SimpleStatement(String.Format("INSERT INTO " + policyTestTools.TableName + " (k, i) VALUES ({0}, {0})", i))
                                .EnableTracing();
                var rs = Session.Execute(statement);
                traces.Add(rs.Info.QueryTrace);
            }
            //Check that there were hops
            var hopsPerQuery = traces.Select(t => t.Events.Any(e => e.Source.ToString() == t.Coordinator.ToString()));
            Assert.True(hopsPerQuery.Any(v => v));
            var tracesPerCoordinator = traces.GroupBy(t => t.Coordinator).ToDictionary(t => t.Key, t => t.Count());
            Assert.AreEqual(3, tracesPerCoordinator.Count);
            Assert.IsTrue(tracesPerCoordinator.All(kvp => kvp.Value == 3));
        }

        [Test]
        public void Should_ThrowSslException_When_ClientCertIsNotProvided()
        {
            var ex = Assert.ThrowsAsync<DriverInternalError>(() => CreateSessionAsync(false));
            AssertIsSslError(ex);
        }

        private void AssertIsSslError(DriverInternalError ex)
        {
#if NETCOREAPP2_0
            var ex2 = ex.InnerException;
            Assert.IsTrue(ex2 is HttpRequestException, ex2.ToString());
            var ex3 = ex2.InnerException;
            if (TestHelper.IsWin)
            {
                Assert.IsTrue(ex3.Message.Contains("A security error occurred"), ex3.Message);
            }
            else
            {
                Assert.IsTrue(ex3.Message.Contains("Authentication failed"), ex3.Message);
            }
#elif NET452
            if (TestHelper.IsMono)
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Authentication failed"), ex2.Message);
                Assert.IsTrue(ex2.Message.Contains("SecureChannelFailure"), ex2.Message);
            }
            else
            {
                var ex2 = ex.InnerException;
                Assert.IsTrue(ex2 is WebException, ex2.ToString());
                Assert.IsTrue(ex2.Message.Contains("Could not create SSL/TLS secure channel."), ex2.Message);
            }
#else
            var ex2 = ex.InnerException;
            Assert.IsTrue(ex2 is HttpRequestException, ex2.ToString());
            Assert.IsTrue(ex2.Message.Contains("The SSL connection could not be established"), ex2.Message);
#endif
        }
    }
}