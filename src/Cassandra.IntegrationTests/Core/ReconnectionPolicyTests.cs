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
using System.Diagnostics;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class ReconnectionPolicyTests : PolicyTestTools
    {
        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        /// <summary>
        /// Test the ExponentialReconnectionPolicy.
        /// </summary>
        [Test]
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
            catch (ArgumentException) { }

            try
            {
                new ExponentialReconnectionPolicy(1, -1);
                Assert.Fail();
            }
            catch (ArgumentException) { }

            try
            {
                new ExponentialReconnectionPolicy(-1, -1);
                Assert.Fail();
            }
            catch (ArgumentException) { }

            try
            {
                new ExponentialReconnectionPolicy(2, 1);
                Assert.Fail();
            }
            catch (ArgumentException) { }

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

            //// Run integration test
            //long restartTime = 2 + 4 + 8 + 2;   // 16: 3 full cycles + 2 seconds
            //long retryTime = 30;                // 4th cycle start time
            //long breakTime = 62;                // time until next reconnection attempt
            //reconnectionPolicyTest(builder, restartTime, retryTime, breakTime);
        }

        /// <summary>
        /// Test the ConstantReconnectionPolicy.
        /// </summary>
        [Test]
        public void constantReconnectionPolicyTest()
        {
            Builder builder = Cluster.Builder().WithReconnectionPolicy(new ConstantReconnectionPolicy(25 * 1000));

            // Ensure that ConstantReconnectionPolicy is what we should be testing
            if (!(builder.GetConfiguration().Policies.ReconnectionPolicy is ConstantReconnectionPolicy))
            {
                Assert.Fail("Set policy does not match retrieved policy.");
            }

            // Test basic getters
            ConstantReconnectionPolicy reconnectionPolicy = (ConstantReconnectionPolicy)builder.GetConfiguration().Policies.ReconnectionPolicy;
            Assert.True(reconnectionPolicy.ConstantDelayMs == 25 * 1000);

            // Test erroneous instantiations
            try
            {
                new ConstantReconnectionPolicy(-1);
                Assert.Fail();
            }
            catch (ArgumentException) { }

            // Test nextDelays()
            var schedule = new ConstantReconnectionPolicy(10 * 1000).NewSchedule();
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);
            Assert.True(schedule.NextDelayMs() == 10000);

            //// Run integration test
            //int restartTime = 32;   
            //int retryTime = 50;        
            //int breakTime = 10;        // time until next reconnection attempt
            //reconnectionPolicyTest(builder, restartTime, retryTime, breakTime);
        }

        public void reconnectionPolicyTest(Builder builder, long restartTime, long retryTime, long breakTime)
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
            var clusterInfo = TestUtils.CcmSetup(1, builder);
            createSchema(clusterInfo.Session, 1);

            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                // Ensure a basic test works
                assertQueried(IpPrefix + "1", 12);
                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 1);

                // Start timing and ensure that the node is down

                //long startTime = 0;
                var startTime = Stopwatch.StartNew();    // = 0;
                try
                {
                    //startTime = System.nanoTime() / 1000000000;                
                    query(clusterInfo, 12);
                    Assert.Fail("Test race condition where node has not shut off quickly enough.");
                }
                catch (NoHostAvailableException) { }

                long elapsedSeconds;
                bool restarted = false;
                while (true)
                {
                    //thisTime = System.nanoTime() / 1000000000;
                    elapsedSeconds = startTime.ElapsedMilliseconds / 1000;

                    // Restart node at restartTime
                    if (!restarted && elapsedSeconds > restartTime)
                    {
                        TestUtils.CcmStart(clusterInfo, 1);
                        restarted = true;
                    }

                    // Continue testing queries each second
                    try
                    {                        
                        query(clusterInfo, 12);
                        assertQueried(IpPrefix + "1", 12);
                        resetCoordinators();

                        // Ensure the time when the query completes successfully is what was expected
                        Assert.True(retryTime - 6 < elapsedSeconds && elapsedSeconds < retryTime + 6, String.Format("Waited {0} seconds instead an expected {1} seconds wait", elapsedSeconds, retryTime));
                    }
                    catch (NoHostAvailableException)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    Thread.Sleep((int)(breakTime * 1000));

                    // The same query once more, just to be sure
                    query(clusterInfo, 12);
                    assertQueried(IpPrefix + "1", 12);
                    resetCoordinators();

                    // Ensure the reconnection times reset
                    TestUtils.CcmStopForce(clusterInfo, 1);

                    // Start timing and ensure that the node is down
                    //startTime = 0;
                    startTime.Reset();
                    try
                    {
                        //startTime = System.nanoTime() / 1000000000;
                        startTime.Start();
                        query(clusterInfo, 12);
                        Assert.Fail("Test race condition where node has not shut off quickly enough.");
                    }
                    catch (NoHostAvailableException) { }

                    restarted = false;
                    while (true)
                    {
                        //elapsedSeconds = System.nanoTime() / 1000000000;
                        elapsedSeconds = startTime.ElapsedMilliseconds / 1000;

                        // Restart node at restartTime
                        if (!restarted && elapsedSeconds > restartTime)
                        {
                            TestUtils.CcmStart(clusterInfo, 1);
                            restarted = true;
                        }

                        // Continue testing queries each second
                        try
                        {
                            query(clusterInfo, 12);
                            assertQueried(IpPrefix + "1", 12);
                            resetCoordinators();

                            // Ensure the time when the query completes successfully is what was expected
                            Assert.True(retryTime - 3 < elapsedSeconds && elapsedSeconds < retryTime + 3, String.Format("Waited {0} seconds instead an expected {1} seconds wait", elapsedSeconds, retryTime));
                        }
                        catch (NoHostAvailableException)
                        {
                            Thread.Sleep(1000);
                            continue;
                        }
                        break;
                    }
                    break;
                }

            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }
    }
}
