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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    public class PolicyTestTools
    {
        public static readonly bool DEBUG = false;

        public static readonly String TABLE = "test";

        public static Dictionary<IPAddress, int> coordinators = new Dictionary<IPAddress, int>();
        public static List<ConsistencyLevel> achievedConsistencies = new List<ConsistencyLevel>();
        public static PreparedStatement prepared;

        /// <summary>
        ///  Create schemas for the policy tests, depending on replication
        ///  factors/strategies.
        /// </summary>
        public static void createSchema(ISession session)
        {
            createSchema(session, 1);
        }

        public static void createSchema(ISession session, int replicationFactor)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, replicationFactor)));
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE)));
        }

        public static void createMultiDCSchema(ISession session, int dc1RF = 1, int dc2RF = 1)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT, TestUtils.SIMPLE_KEYSPACE, "NetworkTopologyStrategy",
                                              string.Format("'dc1' : {0}, 'dc2' : {1}", dc1RF, dc2RF))));
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE)));
        }

        ///  Coordinator management/count
        public static void addCoordinator(IPAddress coordinator, ConsistencyLevel cl)
        {
            if (!coordinators.ContainsKey(coordinator))
                coordinators.Add(coordinator, 0);
            int n = coordinators[coordinator];
            coordinators[coordinator] = n + 1;
            achievedConsistencies.Add(cl);
        }

        public static void resetCoordinators()
        {
            coordinators = new Dictionary<IPAddress, int>();
            achievedConsistencies = new List<ConsistencyLevel>();
        }


        ///  Helper test methodspt
        public static void assertQueried(String host, int n)
        {
            try
            {
                int? queried = coordinators.ContainsKey(IPAddress.Parse(host)) ? (int?) coordinators[IPAddress.Parse(host)] : null;
                if (DEBUG)
                    Debug.WriteLine(String.Format("Expected: {0}\tReceived: {1}", n, queried));
                else
                    Assert.AreEqual(queried == null ? 0 : queried, n, "For " + host);
            }
            catch (Exception e)
            {
                throw new ApplicationException("", e); // RuntimeException(e);
            }
        }

        /// <summary>
        ///     Verifies that exactly n queries were received by the specified set of hosts.  Request distribution within 
        ///     the set is not tested.
        /// </summary>
        public static void assertQueriedSet(String[] hosts, int n)
        {
            try
            {
                int queriedInSet = 0;
                foreach (var host in hosts)
                {
                    queriedInSet += coordinators.ContainsKey(IPAddress.Parse(host)) ? (int)coordinators[IPAddress.Parse(host)] : 0;
                }

                if (DEBUG)
                    Debug.WriteLine(String.Format("Expected: {0}\tReceived: {1}", n, queriedInSet));
                else
                    Assert.AreEqual(queriedInSet, n, String.Format("For [{0}]", String.Join(", ", hosts)));

            }
            catch (Exception e)
            {
                throw new ApplicationException("", e);// RuntimeException(e);
            }
        }

        protected void assertQueriedAtLeast(String host, int n)
        {
            int queried = coordinators[IPAddress.Parse(host)];
            if (DEBUG)
                Debug.WriteLine(String.Format("Expected > {0}\tReceived: {1}", n, queried));
            else
                Assert.True(queried >= n, "For " + host);
        }

        /// <summary>
        /// Asserts that all consistencies achieved in the last execution are equal to the consistency passed 
        /// </summary>
        protected void assertAchievedConsistencyLevel(ConsistencyLevel expectedConsistency)
        {
            Assert.True(achievedConsistencies.All(consistency => consistency == expectedConsistency), "Not all consistencies achieved are " + expectedConsistency);
        }

        /// <summary>
        ///  Init methods that handle writes using batch and consistency options.
        /// </summary>
        protected void init(CcmClusterInfo clusterInfo, int n)
        {
            init(clusterInfo, n, false, ConsistencyLevel.One);
        }

        protected void init(CcmClusterInfo clusterInfo, int n, bool batch)
        {
            init(clusterInfo, n, batch, ConsistencyLevel.One);
        }

        protected void init(CcmClusterInfo clusterInfo, int n, ConsistencyLevel cl)
        {
            init(clusterInfo, n, false, cl);
        }

        protected void init(CcmClusterInfo clusterInfo, int n, bool batch, ConsistencyLevel cl)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test'
            for (int i = 0; i < n; ++i)
                if (batch)
                    // BUG: WriteType == SIMPLE                    
                {
                    var bth = new StringBuilder();
                    bth.AppendLine("BEGIN BATCH");
                    bth.AppendLine(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TestUtils.SIMPLE_TABLE));
                    bth.AppendLine("APPLY BATCH");

                    RowSet qh = clusterInfo.Session.Execute(new SimpleStatement(bth.ToString()).SetConsistencyLevel(cl));
                }
                else
                {
                    RowSet qh =
                        clusterInfo.Session.Execute(
                            new SimpleStatement(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TestUtils.SIMPLE_TABLE)).SetConsistencyLevel(cl));
                }

            prepared = clusterInfo.Session.Prepare("SELECT * FROM " + TestUtils.SIMPLE_TABLE + " WHERE k = ?").SetConsistencyLevel(cl);
        }


        /// <summary>
        ///  Query methods that handle reads based on PreparedStatements and/or
        ///  ConsistencyLevels.
        /// </summary>
        protected void query(CcmClusterInfo clusterInfo, int n)
        {
            query(clusterInfo, n, false, ConsistencyLevel.One);
        }

        protected void query(CcmClusterInfo clusterInfo, int n, bool usePrepared)
        {
            query(clusterInfo, n, usePrepared, ConsistencyLevel.One);
        }

        protected void query(CcmClusterInfo clusterInfo, int n, ConsistencyLevel cl)
        {
            query(clusterInfo, n, false, cl);
        }

        protected void query(CcmClusterInfo clusterInfo, int n, bool usePrepared, ConsistencyLevel cl)
        {
            if (usePrepared)
            {
                BoundStatement bs = prepared.Bind(0);
                for (int i = 0; i < n; ++i)
                {
                    IPAddress ccord;
                    ConsistencyLevel cac;
                    var rs = clusterInfo.Session.Execute(bs);
                    {
                        ccord = rs.Info.QueriedHost;
                        cac = rs.Info.AchievedConsistency;
                    }
                    addCoordinator(ccord, cac);
                }
            }
            else
            {
                var routingKey = new RoutingKey();
                routingKey.RawRoutingKey = Enumerable.Repeat((byte) 0x00, 4).ToArray();
                for (int i = 0; i < n; ++i)
                {
                    IPAddress ccord;
                    ConsistencyLevel cac;
                    var rs =
                            clusterInfo.Session.Execute(
                                new SimpleStatement(String.Format("SELECT * FROM {0} WHERE k = 0", TABLE)).SetRoutingKey(routingKey)
                                                                                                          .SetConsistencyLevel(cl));
                    {
                        ccord = rs.Info.QueriedHost;
                        cac = rs.Info.AchievedConsistency;
                        Trace.TraceInformation("Query {0} executed by {1} with consistency {2}", i, ccord, cac);
                    }
                    addCoordinator(ccord, cac);
                }
            }
        }
    }
}