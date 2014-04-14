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

using System;

namespace Cassandra.IntegrationTests.Core
{
    [TestClass]
    public class LoadBalancingPolicyTests : PolicyTestTools
    {
        [TestMethod]
        [WorksForMe]
        public void roundRobinTestCCM()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, builder);
            createSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 6);
                assertQueried(Options.Default.IP_PREFIX + "2", 6);

                resetCoordinators();
                c.CCMBridge.BootstrapNode(3);
                TestUtils.waitFor(Options.Default.IP_PREFIX + "3", c.Cluster, 60);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 4);
                assertQueried(Options.Default.IP_PREFIX + "2", 4);
                assertQueried(Options.Default.IP_PREFIX + "3", 4);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(1);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "1", c.Cluster, 60);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "2", 6);
                assertQueried(Options.Default.IP_PREFIX + "3", 6);
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
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 3);
                assertQueried(Options.Default.IP_PREFIX + "2", 3);
                assertQueried(Options.Default.IP_PREFIX + "3", 3);
                assertQueried(Options.Default.IP_PREFIX + "4", 3);

                resetCoordinators();
                c.CCMBridge.BootstrapNode(5, "dc2");
                c.CCMBridge.DecommissionNode(1);
                TestUtils.waitFor(Options.Default.IP_PREFIX + "5", c.Cluster, 20);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "1", c.Cluster, 20);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 3);
                assertQueried(Options.Default.IP_PREFIX + "3", 3);
                assertQueried(Options.Default.IP_PREFIX + "4", 3);
                assertQueried(Options.Default.IP_PREFIX + "5", 3);
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
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createMultiDCSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
                assertQueried(Options.Default.IP_PREFIX + "3", 6);
                assertQueried(Options.Default.IP_PREFIX + "4", 6);
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
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
            builder.WithQueryTimeout(10000);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(4, builder);
            createSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);
                resetCoordinators();
                c.CCMBridge.ForceStop(1);
                c.CCMBridge.ForceStop(2);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "1", c.Cluster, 40);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "2", c.Cluster, 40);

                query(c, 12);

                c.CCMBridge.ForceStop(3);
                c.CCMBridge.ForceStop(4);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "3", c.Cluster, 40);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "4", c.Cluster, 40);

                try
                {
                    query(c, 12);
                    Assert.Fail();
                }
                catch (NoHostAvailableException)
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
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);
            createMultiDCSchema(c.Session);
            try
            {
                init(c, 12);
                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
                assertQueried(Options.Default.IP_PREFIX + "3", 6);
                assertQueried(Options.Default.IP_PREFIX + "4", 6);
                assertQueried(Options.Default.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CCMBridge.BootstrapNode(5, "dc3");
                TestUtils.waitFor(Options.Default.IP_PREFIX + "5", c.Cluster, 60);


                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
                assertQueried(Options.Default.IP_PREFIX + "3", 6);
                assertQueried(Options.Default.IP_PREFIX + "4", 6);
                assertQueried(Options.Default.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(3);
                c.CCMBridge.DecommissionNode(4);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "3", c.Cluster, 20);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "4", c.Cluster, 20);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 6);
                assertQueried(Options.Default.IP_PREFIX + "3", 0);
                assertQueried(Options.Default.IP_PREFIX + "4", 0);
                assertQueried(Options.Default.IP_PREFIX + "5", 6);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(5);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "5", c.Cluster, 20);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);
                assertQueried(Options.Default.IP_PREFIX + "3", 0);
                assertQueried(Options.Default.IP_PREFIX + "4", 0);
                assertQueried(Options.Default.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(2);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "2", c.Cluster, 20);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 12);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
                assertQueried(Options.Default.IP_PREFIX + "3", 0);
                assertQueried(Options.Default.IP_PREFIX + "4", 0);
                assertQueried(Options.Default.IP_PREFIX + "5", 0);

                resetCoordinators();
                c.CCMBridge.ForceStop(1);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "2", c.Cluster, 20);

                try
                {
                    query(c, 12);
                    Assert.Fail();
                }
                catch (NoHostAvailableException)
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
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
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
                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);

                resetCoordinators();
                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);

                resetCoordinators();
                c.CCMBridge.ForceStop(2);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "2", c.Cluster, 60);

                try
                {
                    query(c, 12, usePrepared);
                    Assert.Fail();
                }
                catch (UnavailableException)
                {
                }
                catch (ReadTimeoutException)
                {
                }

                resetCoordinators();
                c.CCMBridge.Start(2);
                TestUtils.waitFor(Options.Default.IP_PREFIX + "2", c.Cluster, 60);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(2);
                TestUtils.waitForDecommission(Options.Default.IP_PREFIX + "2", c.Cluster, 60);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 12);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
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
                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);
                assertQueried(Options.Default.IP_PREFIX + "3", 0);

                //resetCoordinators();
                //c.CCMBridge.BootstrapNode(3);
                //TestUtils.waitFor(Options.Default.IP_PREFIX + "3", c.Cluster, 60);

                //query(c, 12);

                //// We should still be hitting only one node
                //assertQueried(Options.Default.IP_PREFIX + "1", 0);
                //assertQueried(Options.Default.IP_PREFIX + "2", 12);
                //assertQueried(Options.Default.IP_PREFIX + "3", 0);

                //resetCoordinators();
                //c.CCMBridge.Stop(2);
                //TestUtils.waitForDown(Options.Default.IP_PREFIX + "2", c.Cluster, 60);

                //query(c, 12);

                //assertQueried(Options.Default.IP_PREFIX + "1", 6);
                //assertQueried(Options.Default.IP_PREFIX + "2", 0);
                //assertQueried(Options.Default.IP_PREFIX + "3", 6);

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