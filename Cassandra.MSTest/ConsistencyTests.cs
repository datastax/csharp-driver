using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;
using System.Threading;

namespace Cassandra.MSTest
{
    public class ConsistencyTests : PolicyTestTools
    {
        [Ignore]
        [TestMethod]
        public void testRFOneTokenAware()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 1);
            try
            {

                init(c, 12, ConsistencyLevel.One);
                query(c, 12, ConsistencyLevel.One);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>() { ConsistencyLevel.Any };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<string> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<string> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFTwoTokenAware()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 2);
            try
            {

                init(c, 12, ConsistencyLevel.Two);
                query(c, 12, ConsistencyLevel.Two);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One
                                                                        };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<string> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<string> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFThreeTokenAware()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 3);
            try
            {

                init(c, 12, ConsistencyLevel.Two);
                query(c, 12, ConsistencyLevel.Two);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum};

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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
        [Ignore]
        [TestMethod]
        public void testRFOneDowngradingCL()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 1);
            try
            {

                init(c, 12, ConsistencyLevel.One);
                query(c, 12, ConsistencyLevel.One);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>() { ConsistencyLevel.Any };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFTwoDowngradingCL()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 2);
            try
            {

                init(c, 12, ConsistencyLevel.Two);
                query(c, 12, ConsistencyLevel.Two);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                // FIXME: This sleep is needed to allow the waitFor() to work
                Thread.Sleep(20000);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All
                                                    };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFThreeRoundRobinDowngradingCL()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy()).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testRFThreeDowngradingCL(builder);
        }

        [Ignore]
        [TestMethod]
        public void testRFThreeTokenAwareDowngradingCL()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testRFThreeDowngradingCL(builder);
        }

        public void testRFThreeDowngradingCL(Builder builder)
        {
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, builder);
            createSchema(c.Session, 3);
            try
            {

                init(c, 12, ConsistencyLevel.All);
                query(c, 12, ConsistencyLevel.All);

                try
                {
                    // This test catches TokenAwarePolicy
                    // However, full tests in LoadBalancingPolicyTest.java
                    assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                    assertQueried(CCMBridge.IP_PREFIX + "2", 12);
                    assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                }
                catch (AssertException e)
                {
                    // This test catches RoundRobinPolicy
                    assertQueried(CCMBridge.IP_PREFIX + "1", 4);
                    assertQueried(CCMBridge.IP_PREFIX + "2", 4);
                    assertQueried(CCMBridge.IP_PREFIX + "3", 4);
                }

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All
                                                    };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum};

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                        "EACH_QUORUM ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFThreeDowngradingCLTwoDCs()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, 3, builder);
            createMultiDCSchema(c.Session, 3, 3);
            try
            {

                init(c, 12, ConsistencyLevel.Two);
                query(c, 12, ConsistencyLevel.Two);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 12);
                assertQueried(CCMBridge.IP_PREFIX + "4", 0);
                assertQueried(CCMBridge.IP_PREFIX + "5", 0);
                assertQueried(CCMBridge.IP_PREFIX + "6", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                // FIXME: This sleep is needed to allow the waitFor() to work
                Thread.Sleep(20000);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);

                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum
                                                    };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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

        [Ignore]
        [TestMethod]
        public void testRFThreeDowngradingCLTwoDCsDCAware()
        {
            var builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc2"))).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(3, 3, builder);
            createMultiDCSchema(c.Session, 3, 3);
            try
            {

                init(c, 12, ConsistencyLevel.Two);
                query(c, 12, ConsistencyLevel.Two);

                assertQueried(CCMBridge.IP_PREFIX + "1", 0);
                assertQueried(CCMBridge.IP_PREFIX + "2", 0);
                assertQueried(CCMBridge.IP_PREFIX + "3", 0);
                // BUG: JAVA-88
                //assertQueried(CCMBridge.IP_PREFIX + "4", 12);
                //assertQueried(CCMBridge.IP_PREFIX + "5", 0);
                //assertQueried(CCMBridge.IP_PREFIX + "6", 0);

                resetCoordinators();
                c.CassandraCluster.ForceStop(2);
                // FIXME: This sleep is needed to allow the waitFor() to work
                Thread.Sleep(20000);
                TestUtils.waitForDownWithWait(CCMBridge.IP_PREFIX + "2", c.Cluster, 5);



                List<ConsistencyLevel> acceptedList = new List<ConsistencyLevel>(){
                                                    ConsistencyLevel.Any,
                                                    ConsistencyLevel.One,
                                                    ConsistencyLevel.Two,
                                                    ConsistencyLevel.Quorum,
                                                    ConsistencyLevel.Three,
                                                    ConsistencyLevel.All,
                                                    ConsistencyLevel.LocalQuorum,
                                                    ConsistencyLevel.EachQuorum
                                                    };

                List<ConsistencyLevel> failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        init(c, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.%s with message: %s", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (var cl in acceptedList)
                {
                    try
                    {
                        query(c, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        List<String> acceptableErrorMessages = new List<string>(){
                        "EACH_QUORUM ConsistencyLevel is only supported for writes",
                        "ANY ConsistencyLevel is only supported for writes"};
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: %s", e.Message));
                    }
                }

                // Test writes which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        init(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (var cl in failList)
                {
                    try
                    {
                        query(c, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.%s.", cl));
                    }
                    catch (ReadTimeoutException e)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException e)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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
    }
}
