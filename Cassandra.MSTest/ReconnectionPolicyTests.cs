using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyTest;
using System.Threading;
using System.Diagnostics;

namespace Cassandra.MSTest
{
    public class ReconnectionPolicyTests : PolicyTestTools
    {
        /*
 * Test the ExponentialReconnectionPolicy.
 */
        
        [TestMethod]
        public void exponentialReconnectionPolicyTest()
        {
            var builder = Cluster.Builder().WithReconnectionPolicy(new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000));

            // Ensure that ExponentialReconnectionPolicy is what we should be testing
            if (!(builder.GetConfiguration().Policies.ReconnectionPolicy is ExponentialReconnectionPolicy))
            {
                Assert.Fail("Set policy does not match retrieved policy.");
            }

            // Test basic getters
            ExponentialReconnectionPolicy reconnectionPolicy = (ExponentialReconnectionPolicy)builder.GetConfiguration().Policies.ReconnectionPolicy;
            Assert.True(reconnectionPolicy.BaseDelayMs == 2 * 1000);
            Assert.True(reconnectionPolicy.MaxDelayMs == 5 * 60 * 1000);

            // Test erroneous instantiations
            try
            {
                new ExponentialReconnectionPolicy(-1, 1);
                Assert.Fail();
            }
            catch (ArgumentException e) { }

            try
            {
                new ExponentialReconnectionPolicy(1, -1);
                Assert.Fail();
            }
            catch (ArgumentException e) { }

            try
            {
                new ExponentialReconnectionPolicy(-1, -1);
                Assert.Fail();
            }
            catch (ArgumentException e) { }

            try
            {
                new ExponentialReconnectionPolicy(2, 1);
                Assert.Fail();
            }
            catch (ArgumentException e) { }

            // Test nextDelays()

            var schedule = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000).NewSchedule();
            Assert.True(schedule.NextDelayMs() == 2000);
            Assert.True(schedule.NextDelayMs() == 4000);
            Assert.True(schedule.NextDelayMs() == 8000);
            Assert.True(schedule.NextDelayMs() == 16000);
            Assert.True(schedule.NextDelayMs() == 32000);
            for (int i = 0; i < 64; ++i)
                schedule.NextDelayMs();
            Assert.True(schedule.NextDelayMs() == reconnectionPolicy.MaxDelayMs);

            // Run integration test
            long restartTime = 2 + 4 + 8 + 2;   // 16: 3 full cycles + 2 seconds
            long retryTime = 30;                // 4th cycle start time
            long breakTime = 62;                // time until next reconnection attempt
            reconnectionPolicyTest(builder, restartTime, retryTime, breakTime);
        }

        /*
         * Test the ConstantReconnectionPolicy.
         */
        [TestMethod]
        public void constantReconnectionPolicyTest()
        {
            Builder builder = Cluster.Builder().WithReconnectionPolicy(new ConstantReconnectionPolicy(10 * 1000));

            // Ensure that ConstantReconnectionPolicy is what we should be testing
            if (!(builder.GetConfiguration().Policies.ReconnectionPolicy is ConstantReconnectionPolicy))
            {
                Assert.Fail("Set policy does not match retrieved policy.");
            }

            // Test basic getters
            ConstantReconnectionPolicy reconnectionPolicy = (ConstantReconnectionPolicy)builder.GetConfiguration().Policies.ReconnectionPolicy;
            Assert.True(reconnectionPolicy.ConstantDelayMs == 10 * 1000);

            // Test erroneous instantiations
            try
            {
                new ConstantReconnectionPolicy(-1);
                Assert.Fail();
            }
            catch (ArgumentException e) { }

            // Test nextDelays()
            var schedule = new ConstantReconnectionPolicy(10 * 1000).NewSchedule();
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);

            // Run integration test
            int restartTime = 16;      // matches the above test
            int retryTime = 20;        // 2nd cycle start time
            int breakTime = 10;        // time until next reconnection attempt
            reconnectionPolicyTest(builder, restartTime, retryTime, breakTime);
        }

        public void reconnectionPolicyTest(Builder builder, long restartTime, long retryTime, long breakTime)
        {
            CCMBridge.CCMCluster c = CCMBridge.CCMCluster.Create(1, builder);
            createSchema(c.Session, 1);

            try
            {
                init(c, 12);
                query(c, 12);

                // Ensure a basic test works
                assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                resetCoordinators();
                c.CassandraCluster.ForceStop(1);

                // Start timing and ensure that the node is down

                //long startTime = 0;
                Stopwatch startTime = Stopwatch.StartNew();    // = 0;
                try
                {
                    //startTime = System.nanoTime() / 1000000000;                
                    query(c, 12);
                    Assert.Fail("Test race condition where node has not shut off quickly enough.");
                }
                catch (NoHostAvailableException e) { }

                long elapsedSeconds;
                bool restarted = false;
                while (true)
                {
                    //thisTime = System.nanoTime() / 1000000000;
                    elapsedSeconds = startTime.ElapsedMilliseconds / 1000;

                    // Restart node at restartTime
                    if (!restarted && elapsedSeconds > restartTime)
                    {
                        c.CassandraCluster.Start(1);
                        restarted = true;
                    }

                    // Continue testing queries each second
                    try
                    {                        
                        query(c, 12);
                        assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                        resetCoordinators();

                        // Ensure the time when the query completes successfully is what was expected
                        Assert.True(retryTime - 2 < elapsedSeconds && elapsedSeconds < retryTime + 2, String.Format("Waited {0} seconds instead an expected {1} seconds wait", elapsedSeconds, retryTime));
                    }
                    catch (NoHostAvailableException e)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    Thread.Sleep((int)(breakTime * 1000));

                    // The same query once more, just to be sure
                    query(c, 12);
                    assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                    resetCoordinators();

                    // Ensure the reconnection times reset
                    c.CassandraCluster.ForceStop(1);

                    // Start timing and ensure that the node is down
                    //startTime = 0;
                    startTime.Reset();
                    try
                    {
                        //startTime = System.nanoTime() / 1000000000;
                        startTime.Start();
                        query(c, 12);
                        Assert.Fail("Test race condition where node has not shut off quickly enough.");
                    }
                    catch (NoHostAvailableException e) { }

                    restarted = false;
                    while (true)
                    {
                        //elapsedSeconds = System.nanoTime() / 1000000000;
                        elapsedSeconds = startTime.ElapsedMilliseconds / 1000;

                        // Restart node at restartTime
                        if (!restarted && elapsedSeconds > restartTime)
                        {
                            c.CassandraCluster.Start(1);
                            restarted = true;
                        }

                        // Continue testing queries each second
                        try
                        {
                            query(c, 12);
                            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
                            resetCoordinators();

                            // Ensure the time when the query completes successfully is what was expected
                            Assert.True(retryTime - 2 < elapsedSeconds && elapsedSeconds < retryTime + 2, String.Format("Waited {0} seconds instead an expected {1} seconds wait", elapsedSeconds, retryTime));
                        }
                        catch (NoHostAvailableException e)
                        {
                            Thread.Sleep(1000);
                            continue;
                        }
                        break;
                    }
                    break;
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
