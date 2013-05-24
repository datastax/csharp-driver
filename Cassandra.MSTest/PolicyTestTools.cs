using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using MyTest;
using System.Diagnostics;
using System.Linq;
using Cassandra.Data.Linq;

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
            Thread.Sleep(3000);
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE));
            Thread.Sleep(1000);
        }

        public static void createMultiDCSchema(Session session, int dc1RF = 1, int dc2RF = 1)
        {
            session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT, TestUtils.SIMPLE_KEYSPACE, "NetworkTopologyStrategy", string.Format("'dc1' : {0}, 'dc2' : {1}",dc1RF,dc2RF) ));
            Thread.Sleep(3000);
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
        //Only for tests purpose:
        //private class InsertQuery : CqlCommand
        //{
        //    string Query;

        //    internal InsertQuery(string query)                
        //    {
        //        this.Query = query;
        //    }

        //    public override string GetCql()
        //    {
        //        return Query;
        //    }
        //}
        protected void init(CCMBridge.CCMCluster c, int n, bool batch, ConsistencyLevel cl)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test'
            for (int i = 0; i < n; ++i)
                if (batch)
                // BUG: WriteType == SIMPLE                    
                {                    
                    //var bth = c.Session.CreateBatch();
                    //bth.Append(new InsertQuery(string.Format("BEGIN BATCH INSERT INTO {0} VALUES {1} APPLY BATCH", TestUtils.SIMPLE_TABLE, "(0,0)")));
                    //bth.SetConsistencyLevel(cl);
                    //bth.Execute();
                    
                    //c.Session.Execute(batch()
                    //        .add(string.Format("INSERT INTO {0} VALUES {1}", TestUtils.SIMPLE_TABLE, new String[] { "k", "i" }, new Object[] { 0, 0 })) //insertInto(SIMPLE_TABLE).values(new String[] { "k", "i" }, new Object[] { 0, 0 }))
                    //        .SetConsistencyLevel(cl));
                }
                else
                    c.Session.Execute(new SimpleStatement(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TestUtils.SIMPLE_TABLE)).SetConsistencyLevel(cl));

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
