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
    public class PreparedStatementsCCMTests
    {
        [Test]
        public void reprepareOnNewlyUpNodeTestCCM()
        {
            reprepareOnNewlyUpNodeTest(true);
        }

        [Test]
        public void reprepareOnNewlyUpNodeNoKeyspaceTestCCM()
        {
            // This is the same test than reprepareOnNewlyUpNodeTest, except that the
            // prepared statement is prepared while no current keyspace is used
            reprepareOnNewlyUpNodeTest(false);
        }

        private void reprepareOnNewlyUpNodeTest(bool useKeyspace)
        {
            string keyspace = "tester";
            var clusterInfo = TestUtils.CcmSetup(2, null, keyspace);
            var cluster = clusterInfo.Cluster;
            var session = clusterInfo.Session;
            try
            {
                string modifiedKs = "";

                if (useKeyspace)
                    session.ChangeKeyspace(keyspace);
                else
                    modifiedKs = keyspace + ".";

                try
                {
                    session.WaitForSchemaAgreement(
                        session.Execute("CREATE TABLE " + modifiedKs + "test(k text PRIMARY KEY, i int)")
                        );
                }
                catch (AlreadyExistsException)
                {
                }
                session.Execute("INSERT INTO " + modifiedKs + "test (k, i) VALUES ('123', 17)");
                session.Execute("INSERT INTO " + modifiedKs + "test (k, i) VALUES ('124', 18)");

                PreparedStatement ps = session.Prepare("SELECT * FROM " + modifiedKs + "test WHERE k = ?");

                var rs = session.Execute(ps.Bind("123"));
                Assert.AreEqual(rs.First().GetValue<int>("i"), 17);

                TestUtils.CcmStopNode(clusterInfo, 1);

                Thread.Sleep(3000);

                TestUtils.CcmStart(clusterInfo, 1);
                Thread.Sleep(40000);

                TestUtils.CcmStopNode(clusterInfo, 2);

                Assert.True(session.Cluster.AllHosts().Select(h => h.IsUp).Count() > 0, "There should be one node up");
                for (var i = 0; i < 10; i++)
                {
                    var rowset = session.Execute(ps.Bind("124"));
                    Assert.AreEqual(rowset.First().GetValue<int>("i"), 18);
                }
            }
            finally
            {
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}