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
using System.Linq;
using System.Text;
using System.Net;
using System.Diagnostics;
using System.Threading;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cassandra.MSTest;
#endif

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
        public void PoliciesAreDifferentInstancesWhenDefault()
        {

            var builder = Cluster.Builder();
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(2, 2, builder);

            using (var cluster1 = builder.WithConnectionString(String.Format("Contact Points={0}1", Options.Default.IP_PREFIX)).Build())
            using (var cluster2 = builder.WithConnectionString(String.Format("Contact Points={0}2", Options.Default.IP_PREFIX)).Build())
            {
                using (var session1 = cluster1.Connect())
                using (var session2 = cluster2.Connect())
                {
                    Assert.True(!Object.ReferenceEquals(session1.Policies.LoadBalancingPolicy, session2.Policies.LoadBalancingPolicy), "Load balancing policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.ReconnectionPolicy, session2.Policies.ReconnectionPolicy), "Reconnection policy instances should be different");
                    Assert.True(!Object.ReferenceEquals(session1.Policies.RetryPolicy, session2.Policies.RetryPolicy), "Retry policy instances should be different");
                }
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

            var builder = Cluster.Builder().WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
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
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy());
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

                var hosts = new string[] { 
                    "unused",
                    Options.Default.IP_PREFIX + "1", 
                    Options.Default.IP_PREFIX + "2", 
                    Options.Default.IP_PREFIX + "3",
                    Options.Default.IP_PREFIX + "4",
                    Options.Default.IP_PREFIX + "5"
                };

                var hostsDc1 = new string[] { hosts[1], hosts[2] };
                var hostsDc2 = new string[] { hosts[3], hosts[4] };

                // verify queries went to local DC; therein distributed equally
                assertQueriedSet(hostsDc1, 0);
                assertQueried(hosts[3], 6);
                assertQueried(hosts[4], 6);
                assertQueried(hosts[5], 0);

                resetCoordinators();
                c.CCMBridge.BootstrapNode(5, "dc3");
                TestUtils.waitFor(hosts[5], c.Cluster, 60);

                query(c, 12);

                // verify queries went to local DC; therein distributed equally
                assertQueriedSet(hostsDc1, 0);
                assertQueried(hosts[3], 6);
                assertQueried(hosts[4], 6);
                assertQueried(hosts[5], 0);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(3);
                c.CCMBridge.DecommissionNode(4);
                TestUtils.waitForDecommission(hosts[3], c.Cluster, 20);
                TestUtils.waitForDecommission(hosts[4], c.Cluster, 20);

                query(c, 12);

                // verify queries distributed equally across remote DCs
                assertQueriedSet(hostsDc1, 6);
                assertQueriedSet(hostsDc2, 0);
                assertQueried(hosts[5], 6);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(5);
                TestUtils.waitForDecommission(hosts[5], c.Cluster, 20);

                query(c, 12);

                // verify queries went to the only live DC
                assertQueriedSet(hostsDc1, 12);
                assertQueriedSet(hostsDc2, 0);
                assertQueried(hosts[5], 0);

                resetCoordinators();
                c.CCMBridge.DecommissionNode(2);
                TestUtils.waitForDecommission(hosts[2], c.Cluster, 20);

                query(c, 12);

                // verify queries went to the only live node
                assertQueried(hosts[1], 12);
                assertQueried(hosts[2], 0);
                assertQueriedSet(hostsDc2, 0);
                assertQueried(hosts[5], 0);

                resetCoordinators();
                c.CCMBridge.ForceStop(1);
                TestUtils.waitForDown(hosts[2], c.Cluster, 20);

                // verify no host exception with all nodes down
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
                catch (UnavailableException e)
                {
                }
                catch (ReadTimeoutException e)
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

                resetCoordinators();
                c.CCMBridge.BootstrapNode(3);
                TestUtils.waitFor(Options.Default.IP_PREFIX + "3", c.Cluster, 60);

                query(c, 12);

                // We should still be hitting only one node
                assertQueried(Options.Default.IP_PREFIX + "1", 0);
                assertQueried(Options.Default.IP_PREFIX + "2", 12);
                assertQueried(Options.Default.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CCMBridge.Stop(2);
                TestUtils.waitForDown(Options.Default.IP_PREFIX + "2", c.Cluster, 60);

                query(c, 12);

                assertQueried(Options.Default.IP_PREFIX + "1", 6);
                assertQueried(Options.Default.IP_PREFIX + "2", 0);
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
    }
}
    
    
