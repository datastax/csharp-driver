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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.FoundBugs
{
    [TestFixture, Category("long"), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class FoundBugTests : TestGlobals
    {
        [Test]
        public void Jira_CSHARP_80_82()
        {
            try
            {
                using (var cluster = Cluster.Builder().AddContactPoint("0.0.0.0").Build())
                {
                    try
                    {
                        using (cluster.Connect())
                        {
                        }
                    }
                    catch (NoHostAvailableException)
                    {
                    }
                    catch
                    {
                        Assert.Fail("NoHost expected");
                    }
                }
            }
            catch (NullReferenceException)
            {
                Assert.Fail("Null pointer!");
            }
        }

        //During reconnect the table name becomes invalid
        [Test]
        public void Jira_CSHARP_40()
        {
            ITestCluster nonShareableTestCluster = TestClusterManager.GetNonShareableTestCluster(1);
            var session = nonShareableTestCluster.Session;
            string keyspaceName = "excelsior";
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            const string cqlQuery = "SELECT * from system.local";
            var query = new SimpleStatement(cqlQuery).EnableTracing();
            {
                var result = session.Execute(query);
                Assert.Greater(result.Count(), 0, "It should return rows");
            }

            nonShareableTestCluster.StopForce(1);

            // now wait until node is down
            bool noHostAvailableExceptionWasCaught = false;
            while (!noHostAvailableExceptionWasCaught)
            {
                try
                {
                    nonShareableTestCluster.Cluster.Connect();
                }
                catch (Exception e)
                {
                    if (e.GetType() == typeof (Cassandra.NoHostAvailableException))
                    {
                        noHostAvailableExceptionWasCaught = true;
                    }
                    else
                    {
                        Trace.TraceWarning("Something other than a NoHostAvailableException was thrown: " + e.GetType() + ", waiting another second ...");
                        Thread.Sleep(1000);
                    }
                }
            }

            // now restart the node
            nonShareableTestCluster.Start(1);
            bool hostWasReconnected = false;
            DateTime timeInTheFuture = DateTime.Now.AddSeconds(20);
            while (!hostWasReconnected && DateTime.Now < timeInTheFuture)
            {
                try
                {
                    session.Execute(query);
                    hostWasReconnected = true;
                }
                catch (Exception e)
                {
                    if (e.GetType() == typeof (Cassandra.NoHostAvailableException))
                    {
                        Trace.TraceInformation("Host still not up yet, waiting another one second ... ");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
            RowSet rowSet = session.Execute(query);
            Assert.True(rowSet.GetRows().Count() > 0, "It should return rows");
        }
    }
}
