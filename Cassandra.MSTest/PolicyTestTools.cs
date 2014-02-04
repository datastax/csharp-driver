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
﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.Diagnostics;
using System.Linq;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

namespace Cassandra.MSTest
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
        public static void createSchema(Session session)
        {
            createSchema(session, 1);
        }

        public static void createSchema(Session session, int replicationFactor)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, replicationFactor)));
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE)));
        }

        public static void createMultiDCSchema(Session session, int dc1RF = 1, int dc2RF = 1)
        {
            session.WaitForSchemaAgreement(
                session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT, TestUtils.SIMPLE_KEYSPACE, "NetworkTopologyStrategy", string.Format("'dc1' : {0}, 'dc2' : {1}", dc1RF, dc2RF))));
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.WaitForSchemaAgreement(
                session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE)));
        }

        ///  Coordinator management/count

        public static void addCoordinator(IPAddress coordinator, ConsistencyLevel cl)
        {
            if (!coordinators.ContainsKey(coordinator))
                coordinators.Add(coordinator, 0);
            var n = coordinators[coordinator];
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
                int? queried = coordinators.ContainsKey(IPAddress.Parse(host)) ? (int?)coordinators[IPAddress.Parse(host)] : null;
                if (DEBUG)
                    Debug.WriteLine(String.Format("Expected: {0}\tReceived: {1}", n, queried));
                else
                    Assert.Equal(queried == null ? 0 : queried, n, "For " + host);
            }
            catch (Exception e)
            {
                throw new ApplicationException("", e);// RuntimeException(e);
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
                    Assert.Equal(queriedInSet, n, String.Format("For [{0}]", String.Join(", ", hosts)));

            }
            catch (Exception e)
            {
                throw new ApplicationException("", e);// RuntimeException(e);
            }
        }

        protected void assertQueriedAtLeast(String host, int n)
        {
            try
            {
                int queried = coordinators[IPAddress.Parse(host)];
                if (DEBUG)
                    Debug.WriteLine(String.Format("Expected > {0}\tReceived: {1}", n, queried));
                else
                    Assert.True(queried >= n, "For " + host);
            }
            catch (Exception e)
            {
                throw;
            }
        }

        protected void assertAchievedConsistencyLevel(ConsistencyLevel cl)
        {
            foreach (var c in achievedConsistencies)
                Assert.Equal(cl, c);
        }

        /// <summary>
        ///  Init methods that handle writes using batch and consistency options.
        /// </summary>

        protected void init(CCMBridge.CCMCluster c, int n)
        {
            init(c, n, false, ConsistencyLevel.One);
        }

        protected void init(CCMBridge.CCMCluster c, int n, bool batch)
        {
            init(c, n, batch, ConsistencyLevel.One);
        }

        protected void init(CCMBridge.CCMCluster c, int n, ConsistencyLevel cl)
        {
            init(c, n, false, cl);
        }
        protected void init(CCMBridge.CCMCluster c, int n, bool batch, ConsistencyLevel cl)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test'
            for (int i = 0; i < n; ++i)
                if (batch)
                // BUG: WriteType == SIMPLE                    
                {
                    StringBuilder bth = new StringBuilder();
                    bth.AppendLine("BEGIN BATCH");
                    bth.AppendLine(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TestUtils.SIMPLE_TABLE));
                    bth.AppendLine("APPLY BATCH");

                    var qh = c.Session.Execute(new SimpleStatement(bth.ToString()).SetConsistencyLevel(cl));
                }
                else
                {
                    var qh = c.Session.Execute(new SimpleStatement(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TestUtils.SIMPLE_TABLE)).SetConsistencyLevel(cl));
                }

            prepared = c.Session.Prepare("SELECT * FROM " + TestUtils.SIMPLE_TABLE + " WHERE k = ?").SetConsistencyLevel(cl);
        }


        /// <summary>
        ///  Query methods that handle reads based on PreparedStatements and/or
        ///  ConsistencyLevels.
        /// </summary>
        protected void query(CCMBridge.CCMCluster c, int n)
        {
            query(c, n, false, ConsistencyLevel.One);
        }

        protected void query(CCMBridge.CCMCluster c, int n, bool usePrepared)
        {
            query(c, n, usePrepared, ConsistencyLevel.One);
        }

        protected void query(CCMBridge.CCMCluster c, int n, ConsistencyLevel cl)
        {
            query(c, n, false, cl);
        }

        protected void query(CCMBridge.CCMCluster c, int n, bool usePrepared, ConsistencyLevel cl)
        {
            if (usePrepared)
            {
                BoundStatement bs = prepared.Bind(0);
                for (int i = 0; i < n; ++i)
                {
                    IPAddress ccord;
                    ConsistencyLevel cac;
                    using (var rs = c.Session.Execute(bs))
                    {
                        ccord = rs.Info.QueriedHost;
                        cac = rs.Info.AchievedConsistency;
                    }
                    addCoordinator(ccord, cac);
                }
            }
            else
            {
                RoutingKey routingKey = new RoutingKey();
                routingKey.RawRoutingKey = Enumerable.Repeat((byte)0x00, 4).ToArray();
                for (int i = 0; i < n; ++i)
                {
                    IPAddress ccord;
                    ConsistencyLevel cac;
                    using (var rs = c.Session.Execute(new SimpleStatement(String.Format("SELECT * FROM {0} WHERE k = 0", TABLE)).SetRoutingKey(routingKey).SetConsistencyLevel(cl)))
                    {
                        ccord = rs.Info.QueriedHost;
                        cac = rs.Info.AchievedConsistency;
                        Console.WriteLine(string.Format("Query {0} executed by {1} with consistency {2}", i.ToString(), ccord.ToString(), cac.ToString()));                        
                    }
                    addCoordinator(ccord, cac);
                }
            }
        }
    }
}
