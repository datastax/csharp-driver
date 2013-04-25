using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;
using System.Net;
using System.Diagnostics;
using System.Threading;

namespace Cassandra.MSTest
{
    [TestClass]
    public class LoadBalancingPolicyTests
    {
        private static readonly bool DEBUG = false;

        private static readonly String TABLE = "test";

        private Dictionary<IPAddress, int> coordinators = new Dictionary<IPAddress, int>();
        private PreparedStatement prepared;

        private void createSchema(Session session)
        {
            createSchema(session, 1);
        }

        private void createSchema(Session session, int replicationFactor)
        {
            session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, replicationFactor), ConsistencyLevel.All);
            Thread.Sleep(1000); 
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE));
            Thread.Sleep(1000);
        }

        private void createMultiDCSchema(Session session)
        {

            session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_GENERIC_FORMAT, TestUtils.SIMPLE_KEYSPACE, "NetworkTopologyStrategy", "'dc1' : 1, 'dc2' : 1"));
            session.ChangeKeyspace(TestUtils.SIMPLE_KEYSPACE);
            session.Execute(String.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", TABLE));
        }

        private void addCoordinator(CqlRowSet rs)
        {
            IPAddress coordinator = rs.QueriedHost;
            if (!coordinators.ContainsKey(coordinator))
                coordinators.Add(coordinator, 0);
            var n = coordinators[coordinator];
            coordinators[coordinator] = n + 1;
        }

        private void assertQueried(String host, int n)
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
                throw new ApplicationException("",e);// RuntimeException(e);
            }
        }

        private void resetCoordinators()
        {
            coordinators = new Dictionary<IPAddress, int>();
        }

        private void init(CCMBridge.CCMCluster c, int n)
        {
            // We don't use insert for our test because the resultSet don't ship the queriedHost
            // Also note that we don't use tracing because this would trigger requests that screw up the test'
            for (int i = 0; i < n; ++i)
                c.Session.Execute(String.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", TABLE));

            prepared = c.Session.Prepare("SELECT * FROM " + TABLE + " WHERE k = ?");
        }

        private void query(CCMBridge.CCMCluster c, int n)
        {
            query(c, n, false);
        }

        private void query(CCMBridge.CCMCluster c, int n, bool usePrepared)
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
                routingKey.RawRoutingKey =Enumerable.Repeat((byte)0x00, 4).ToArray();                                                
                for (int i = 0; i < n; ++i)                   
                    addCoordinator(c.Session.Execute(new SimpleStatement(String.Format("SELECT * FROM {0} WHERE k = 0", TABLE)).SetRoutingKey(routingKey)));
                
            }
        }

        [TestMethod]
        [Priority]
        public void roundRobinTest()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, builder);
            createSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 6);
                assertQueried(CCMBridge.IP_PREFIX + "2", 6);

                resetCoordinators();
                c.CassandraCluster.BootstrapNode(3);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "3", c.Cluster, 60);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 4);
                assertQueried(CCMBridge.IP_PREFIX + "2", 4);
                assertQueried(CCMBridge.IP_PREFIX + "3", 4);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(1);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "1", c.Cluster, 60);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "2", 6);
                assertQueried(CCMBridge.IP_PREFIX + "3", 6);

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }

        [TestMethod]
        public void roundRobinWith2DCsTest()
        {

            var builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createSchema(c.Session);
            try
            {

                init(c, 12);
                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 3);
                assertQueried(CCMBridge.IP_PREFIX + "2", 3);
                assertQueried(CCMBridge.IP_PREFIX + "3", 3);
                assertQueried(CCMBridge.IP_PREFIX + "4", 3);

                resetCoordinators();
                c.CassandraCluster.BootstrapNode(5, "dc2");
                c.CassandraCluster.DecommissionNode(1);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "5", c.Cluster, 20);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "1", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 3);
                assertQueried(CCMBridge.IP_PREFIX + "3", 3);
                assertQueried(CCMBridge.IP_PREFIX + "4", 3);
                assertQueried(CCMBridge.IP_PREFIX + "5", 3);

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }

        [TestMethod]
        public void DCAwareRoundRobinTest()
        {

            var builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createMultiDCSchema(c.Session);
            try
            {

                init(c, 12);
                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 6);
                assertQueried(CCMBridge.IP_PREFIX + "4", 6);

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }

        [TestMethod]
        public void dcAwareRoundRobinTestWithOneRemoteHost()
        {

            var builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createMultiDCSchema(c.Session);
            try
            {

                init(c, 12);
                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 6);
                assertQueried(CCMBridge.IP_PREFIX + "4", 6);
                assertQueried(CCMBridge.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CassandraCluster.BootstrapNode(5, "dc3");
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "5", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 6);
                assertQueried(CCMBridge.IP_PREFIX + "4", 6);
                assertQueried(CCMBridge.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(3);
                c.CassandraCluster.DecommissionNode(4);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "3", c.Cluster, 20);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "4", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);
                assertQueried(CCMBridge.IP_PREFIX + "5", 12);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(5);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "5", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);
                assertQueried(CCMBridge.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(2);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);
                assertQueried(CCMBridge.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(1);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "1", c.Cluster, 20);

                try
                {
                    query(c, 12);
                    Assert.Fail();                    
                }
                catch (NoHostAvailableException e)
                {
                    // No more nodes so ...
                }

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }

        [TestMethod]
        public void tokenAwareTest()
        {
            tokenAwareTest(false);
        }

        [TestMethod]
        public void tokenAwarePreparedTest()
        {
            tokenAwareTest(true);
        }

        public void tokenAwareTest(bool usePrepared)
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, builder);
            createSchema(c.Session);
            try
            {

                init(c, 12);
                query(c, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);

                resetCoordinators();
                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

                try
                {
                    query(c, 12, usePrepared);
                    Assert.Fail();
                }
                catch (UnavailableException e)
                {
                    Assert.Equal("Not enough replica available for query at consistency ONE (1 required but only 0 alive)",
                                 e.Message);
                }

                resetCoordinators();
                c.CassandraCluster.Start(2);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(2);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }

        [TestMethod]
        public void tokenAwareWithRF2Test()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, builder);
            createSchema(c.Session, 2);
            try
            {

                init(c, 12);
                query(c, 12);

                // Not the best test ever, we should use OPP and check we do it the
                // right nodes. But since M3P is hard-coded for now, let just check
                // we just hit only one node.
                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.BootstrapNode(3);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "3", c.Cluster, 20);

                query(c, 12);

                // We should still be hitting only one node
                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.Stop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 6);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 6);

            }
            catch (Exception e)
            {
                c.ErrorOut();
                throw e;
            }
            finally
            {
                resetCoordinators();
                c.Discard();
            }
        }
    }
}
    
    
