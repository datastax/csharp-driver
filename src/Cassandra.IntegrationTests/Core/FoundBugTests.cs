//
//      Copyright (C) 2012 DataStax Inc.
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

using NUnit.Framework;
using System;
using System.Linq;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class FoundBugTests
    {
        [Test]
        public void Jira_CSHARP_80_82()
        {
            try
            {
                using (Cluster cluster = Cluster.Builder().AddContactPoint("0.0.0.0").Build())
                {
                    ISession session = null;
                    try
                    {
                        using (session = cluster.Connect())
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

        [Test]
        public void Jira_CSHARP_40()
            //During reconnect the tablespace name becomes invalid
        {
            var clusterInfo = TestUtils.CcmSetup(2);
            try
            {
                var Session = clusterInfo.Session;
                string Keyspace = "Excelsior";
                Session.CreateKeyspaceIfNotExists(Keyspace);
                Session.ChangeKeyspace(Keyspace);
                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";
                var query = new SimpleStatement(cqlKeyspaces).EnableTracing();
                {
                    var result = Session.Execute(query);
                    Assert.True(result.Count() > 0, "It should return rows");
                }

                TestUtils.CcmStopForce(clusterInfo, 1);
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDown("127.0.0.1", clusterInfo.Cluster, 40);
                TestUtils.waitForDown("127.0.0.2", clusterInfo.Cluster, 40);

                try
                {
                    var result = Session.Execute(query);
                    Assert.True(result.Count() > 0, "It should return rows");
                }
                catch (Exception)
                {
                }


                TestUtils.CcmStart(clusterInfo, 1);
                Thread.Sleep(35000);
                TestUtils.waitFor("127.0.0.1", clusterInfo.Cluster, 60);

                {
                    RowSet result = Session.Execute(query);
                    Assert.True(result.Count() > 0, "It should return rows");
                }
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}