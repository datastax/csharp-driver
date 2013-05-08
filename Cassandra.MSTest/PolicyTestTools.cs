using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using MyTest;
using System.Diagnostics;
using System.Linq;

namespace Cassandra.MSTest
{
    public class PolicyTestTools
    {
        public static readonly bool DEBUG = false;

        public static readonly String TABLE = "test";

        public static Dictionary<IPAddress, int> coordinators = new Dictionary<IPAddress, int>();
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
            session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, replicationFactor), ConsistencyLevel.All);
            Thread.Sleep(1000);
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE));
            Thread.Sleep(1000);
        }

        public static void createMultiDCSchema(Session session)
        {
            session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT, TestUtils.SIMPLE_KEYSPACE, "NetworkTopologyStrategy", "'dc1' : 1, 'dc2' : 1"));
            Thread.Sleep(1000);
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE));
            Thread.Sleep(1000);
        }

        ///  Coordinator management/count
        
        public static void addCoordinator(CqlRowSet rs)
        {
            IPAddress coordinator = rs.QueriedHost;
            if (!coordinators.ContainsKey(coordinator))
                coordinators.Add(coordinator, 0);
            var n = coordinators[coordinator];
            coordinators[coordinator] = n + 1;
        }
        public static void resetCoordinators()
        {
            coordinators = new Dictionary<IPAddress, int>();
        }


		///  Helper test methods

       
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
        protected void assertQueriedAtLeast(String host, int n)
        {
            try
            {
                int queried = coordinators[IPAddress.Parse(host)];
                queried = queried == null ? 0 : queried;
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


        public static void init(CCMBridge.CCMCluster c, int n)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test'
            for (int i = 0; i < n; ++i)
                c.Session.Execute(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TABLE));

            prepared = c.Session.Prepare("SELECT * FROM " + TABLE + " WHERE k = ?");
        }

    		/// <summary>
		///  Query methods that handle reads based on PreparedStatements and/or
		///  ConsistencyLevels.
		/// </summary>
        public static void query(CCMBridge.CCMCluster c, int n)
        {
            query(c, n, false);
        }

        public static void query(CCMBridge.CCMCluster c, int n, bool usePrepared)
        {
            if (usePrepared)
            {
                BoundStatement bs = prepared.Bind(0);
                for (int i = 0; i < n; ++i)
                    addCoordinator(c.Session.Execute(bs));
            }
            else
            {
                CassandraRoutingKey routingKey = new CassandraRoutingKey();
                routingKey.RawRoutingKey = Enumerable.Repeat((byte)0x00, 4).ToArray();
                for (int i = 0; i < n; ++i)
                    addCoordinator(c.Session.Execute(new SimpleStatement(String.Format("SELECT * FROM {0} WHERE k = 0", TABLE)).SetRoutingKey(routingKey)));

            }
        }
    }
}
