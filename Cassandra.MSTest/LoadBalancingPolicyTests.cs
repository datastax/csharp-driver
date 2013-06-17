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
    public class LoadBalancingPolicyTests : PolicyTestTools
    {

        [TestMethod]
		[WorksForMe]
        public void roundRobinTestCCM()
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
        [WorksForMe]
        public void roundRobinWith2DCsTestCCM()
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
		[WorksForMe]
        public void DCAwareRoundRobinTestCCM()
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
		[WorksForMe]
        public void forceStopCCM()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            builder.WithQueryTimeout(10000);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(4, builder);
            createSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);
                resetCoordinators();
                c.CassandraCluster.ForceStop(1);
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "1", c.Cluster, 40);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 40);

                query(c, 12);

                c.CassandraCluster.ForceStop(3);
                c.CassandraCluster.ForceStop(4);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "3", c.Cluster, 40);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "4", c.Cluster, 40);

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
        [WorksForMe]
        public void dcAwareRoundRobinTestWithOneRemoteHostCCM()
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
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "5", c.Cluster, 60);


                query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);
            assertQueried(CCMBridge.IP_PREFIX + "4", 6);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(3);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "3", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 6);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 6);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(4);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "4", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(1);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "1", c.Cluster, 20);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 20);

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
		[WorksForMe]
        public void tokenAwareTestCCM()
        {
            tokenAwareTest(false);
        }

        [TestMethod]
        [WorksForMe]
        public void tokenAwarePreparedTestCCM()
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
                //c.Cluster.RefreshSchema();
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
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 60);

                try
                {
                    query(c, 12, usePrepared);
                    Assert.Fail();
                }
                catch (UnavailableException e)
                {
                }
                catch (ReadTimeoutException e)
                {
                }

                resetCoordinators();
                c.CassandraCluster.Start(2);
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "2", c.Cluster, 60);

                query(c, 12);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);

                resetCoordinators();
                c.CassandraCluster.DecommissionNode(2);
                TestUtils.waitForDecommission(CCMBridge.IP_PREFIX + "2", c.Cluster, 60);

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
        [WorksForMe]
        public void tokenAwareWithRF2TestCCM()
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
                TestUtils.waitFor(CCMBridge.IP_PREFIX + "3", c.Cluster, 60);

                query(c, 12);

                // We should still be hitting only one node
                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.Stop(2);
                TestUtils.waitForDown(CCMBridge.IP_PREFIX + "2", c.Cluster, 60);

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
    
    
