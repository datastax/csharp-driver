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
using System.Threading;
using Cassandra.IntegrationTests.Core.Policies;
using NUnit.Framework;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class RetryPolicyTests : PolicyTestTools
    {
        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        [Test]
        public void defaultRetryPolicy()
        {
            Builder builder = Cluster.Builder();
            defaultPolicyTest(builder);
        }

        [Test]
        public void defaultLoggingPolicy()
        {
            var builder = Cluster.Builder().WithRetryPolicy(new LoggingRetryPolicy(new DefaultRetryPolicy()));
            defaultPolicyTest(builder);
        }

        /*
         * Test the FallthroughRetryPolicy.
         * Uses the same code that DefaultRetryPolicy uses.
         */
        [Test]
        public void fallthroughRetryPolicy()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(FallthroughRetryPolicy.Instance);
            defaultPolicyTest(builder);
        }

        /*
         * Test the FallthroughRetryPolicy with Logging enabled.
         * Uses the same code that DefaultRetryPolicy uses.
         */
        [Test]
        public void fallthroughLoggingPolicy()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.Instance));
            defaultPolicyTest(builder);
        }

        public void defaultPolicyTest(Builder builder)
        {
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session);

            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers with fallthrough*Policy
            Thread.Sleep(5000);
            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 6);
                assertQueried(IpPrefix + "2", 6);

                resetCoordinators();

                // Test reads
                bool successfulQuery = false;
                bool readTimeoutOnce = false;
                bool unavailableOnce = false;
                bool restartOnce = false;
                for (int i = 0; i < 100; ++i)
                {
                    try
                    {
                        // Force a ReadTimeoutException to be performed once
                        if (!readTimeoutOnce)
                        {
                            TestUtils.CcmStopForce(clusterInfo, 2);
                        }

                        // Force an UnavailableException to be performed once
                        if (readTimeoutOnce && !unavailableOnce)
                        {
                            TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);
                        }

                        // Bring back node to ensure other errors are not thrown on restart
                        if (unavailableOnce && !restartOnce)
                        {
                            TestUtils.CcmStart(clusterInfo, 2);
                            restartOnce = true;
                        }

                        query(clusterInfo, 12);

                        if (restartOnce)
                            successfulQuery = true;
                    }
                    catch (UnavailableException)
                    {
                        //                        Assert.Equal("Not enough replica available for query at consistency ONE (1 required but only 0 alive)".ToLower(), e.Message.ToLower());
                        unavailableOnce = true;
                    }
                    catch (ReadTimeoutException)
                    {
                        //                        Assert.Equal("Cassandra timeout during read query at consistency ONE (1 responses were required but only 0 replica responded)".ToLower(), e.Message.ToLower());
                        readTimeoutOnce = true;
                    }
                }

                // Ensure the full cycle was completed
                Assert.True(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
                Assert.True(readTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
                Assert.True(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

                // A weak test to ensure that the nodes were contacted
                assertQueriedAtLeast(IpPrefix + "1", 1);
                assertQueriedAtLeast(IpPrefix + "2", 1);

                resetCoordinators();


                // Test writes
                successfulQuery = false;
                bool writeTimeoutOnce = false;
                unavailableOnce = false;
                restartOnce = false;
                for (int i = 0; i < 100; ++i)
                {
                    try
                    {
                        // Force a WriteTimeoutException to be performed once
                        if (!writeTimeoutOnce)
                        {
                            TestUtils.CcmStopForce(clusterInfo, 2);
                        }

                        // Force an UnavailableException to be performed once
                        if (writeTimeoutOnce && !unavailableOnce)
                        {
                            TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);
                        }

                        // Bring back node to ensure other errors are not thrown on restart
                        if (unavailableOnce && !restartOnce)
                        {
                            TestUtils.CcmStart(clusterInfo, 2);
                            restartOnce = true;
                        }

                        init(clusterInfo, 12);

                        if (restartOnce)
                            successfulQuery = true;
                    }
                    catch (UnavailableException)
                    {
                        //                        Assert.Equal("Not enough replica available for query at consistency ONE (1 required but only 0 alive)".ToLower(), e.Message.ToLower());
                        unavailableOnce = true;
                    }
                    catch (WriteTimeoutException)
                    {
                        //                        Assert.Equal("Cassandra timeout during write query at consistency ONE (1 replica were required but only 0 acknowledged the write)".ToLower(), e.Message.ToLower());
                        writeTimeoutOnce = true;
                    }
                }
                // Ensure the full cycle was completed
                Assert.True(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
                Assert.True(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
                Assert.True(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

                // TODO: Missing test to see if nodes were written to

                // Test batch writes
                successfulQuery = false;
                writeTimeoutOnce = false;
                unavailableOnce = false;
                restartOnce = false;
                for (int i = 0; i < 100; ++i)
                {
                    try
                    {
                        // Force a WriteTimeoutException to be performed once
                        if (!writeTimeoutOnce)
                        {
                            TestUtils.CcmStopForce(clusterInfo, 2);
                        }

                        // Force an UnavailableException to be performed once
                        if (writeTimeoutOnce && !unavailableOnce)
                        {
                            TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);
                        }

                        // Bring back node to ensure other errors are not thrown on restart
                        if (unavailableOnce && !restartOnce)
                        {
                            TestUtils.CcmStart(clusterInfo, 2);
                            restartOnce = true;
                        }

                        init(clusterInfo, 12, true);

                        if (restartOnce)
                            successfulQuery = true;
                    }
                    catch (UnavailableException)
                    {
                        //                        Assert.Equal("Not enough replica available for query at consistency ONE (1 required but only 0 alive)", e.Message);
                        unavailableOnce = true;
                    }
                    catch (WriteTimeoutException)
                    {
                        //                        Assert.Equal("Cassandra timeout during write query at consistency ONE (1 replica were required but only 0 acknowledged the write)", e.Message);
                        writeTimeoutOnce = true;
                    }
                }
                // Ensure the full cycle was completed
                Assert.True(successfulQuery, "Hit testing race condition. [Never completed successfully.] (Shouldn't be an issue.):\n");
                Assert.True(writeTimeoutOnce, "Hit testing race condition. [Never encountered a ReadTimeoutException.] (Shouldn't be an issue.):\n");
                Assert.True(unavailableOnce, "Hit testing race condition. [Never encountered an UnavailableException.] (Shouldn't be an issue.):\n");

                // TODO: Missing test to see if nodes were written to
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy
        /// </summary>
        [Test]
        public void DowngradingConsistencyRetryPolicyTest()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            DowngradingConsistencyRetryPolicyTest(builder);
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy with LoggingRetryPolicy
        /// </summary>
        [Test]
        public void DowngradingConsistencyLoggingPolicyTest()
        {
            Builder builder = Cluster.Builder().WithRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.Instance));
            DowngradingConsistencyRetryPolicyTest(builder);
        }
        /// <summary>
        /// Unit test on retry decisions
        /// </summary>
        [Test]
        public void DowngradingConsistencyRetryTest()
        {
            //Retry if 1 of 2 replicas are alive
            var decision = Session.GetRetryDecision(null, new UnavailableException(ConsistencyLevel.Two, 2, 1), DowngradingConsistencyRetryPolicy.Instance, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Retry if 2 of 3 replicas are alive
            decision = Session.GetRetryDecision(null, new UnavailableException(ConsistencyLevel.Three, 3, 2), DowngradingConsistencyRetryPolicy.Instance, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Throw if 0 replicas are alive
            decision = Session.GetRetryDecision(null, new UnavailableException(ConsistencyLevel.Three, 3, 0), DowngradingConsistencyRetryPolicy.Instance, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Rethrow);

            //Retry if 1 of 3 replicas is alive
            decision = Session.GetRetryDecision(null, new ReadTimeoutException(ConsistencyLevel.All, 3, 1, false), DowngradingConsistencyRetryPolicy.Instance, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);
        }

        /// <summary>
        ///  Tests DowngradingConsistencyRetryPolicy
        /// </summary>
        public void DowngradingConsistencyRetryPolicyTest(Builder builder)
        {
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 3);

            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            Thread.Sleep(2000);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.All);

                query(clusterInfo, 12, ConsistencyLevel.All);
                assertAchievedConsistencyLevel(ConsistencyLevel.All);

                //Kill one node: 2 nodes alive
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 20);

                Thread.Sleep(5000);

                //After killing one node, the achieved consistency level should be downgraded
                resetCoordinators();
                query(clusterInfo, 12, ConsistencyLevel.All);
                assertAchievedConsistencyLevel(ConsistencyLevel.Two);

            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        /*
         * Test the AlwaysIgnoreRetryPolicy with Logging enabled.
         */
        [Test]
        public void AlwaysIgnoreRetryPolicyTest()
        {
            var builder = Cluster.Builder()
                .WithRetryPolicy(new LoggingRetryPolicy(AlwaysIgnoreRetryPolicy.Instance))
                .AddContactPoint(IpPrefix + "1")
                .AddContactPoint(IpPrefix + "2");
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session);

            try
            {
                Thread.Sleep(3000);

                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 6);
                assertQueried(IpPrefix + "2", 6);

                resetCoordinators();

                // Test failed reads
                TestUtils.CcmStopForce(clusterInfo, 2);
                for (int i = 0; i < 10; ++i)
                {
                    query(clusterInfo, 12);
                }

                // A weak test to ensure that the nodes were contacted
                assertQueried(IpPrefix + "1", 120);
                assertQueried(IpPrefix + "2", 0);
                resetCoordinators();


                TestUtils.CcmStart(clusterInfo, 2);
                TestUtils.waitFor(IpPrefix + "2", clusterInfo.Cluster, 30);

                Thread.Sleep(5000);

                // Test successful reads
                for (int i = 0; i < 10; ++i)
                {
                    query(clusterInfo, 12);
                }

                // A weak test to ensure that the nodes were contacted
                assertQueriedAtLeast(IpPrefix + "1", 1);
                assertQueriedAtLeast(IpPrefix + "2", 1);
                resetCoordinators();


                // Test writes
                for (int i = 0; i < 100; ++i)
                {
                    init(clusterInfo, 12);
                }

                // TODO: Missing test to see if nodes were written to


                // Test failed writes
                TestUtils.CcmStopForce(clusterInfo, 2);
                for (int i = 0; i < 100; ++i)
                {
                    init(clusterInfo, 12);
                }

                // TODO: Missing test to see if nodes were written to
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }


        /*
         * Test the AlwaysIgnoreRetryPolicy with Logging enabled.
         */
        [Test]
        public void AlwaysRetryRetryPolicyTest()
        {
            Trace.TraceInformation("MainThread is");
            Trace.TraceInformation("[");
            Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
            Trace.TraceInformation("]");

            var builder = Cluster.Builder()
                .WithRetryPolicy(new LoggingRetryPolicy(AlwaysRetryRetryPolicy.Instance))
                .AddContactPoint(IpPrefix + "1")
                .AddContactPoint(IpPrefix + "2");
            var clusterInfo = TestUtils.CcmSetup(2, builder);
            createSchema(clusterInfo.Session);

            try
            {
                init(clusterInfo, 12);
                query(clusterInfo, 12);

                assertQueried(IpPrefix + "1", 6);
                assertQueried(IpPrefix + "2", 6);

                resetCoordinators();

                // Test failed reads
                TestUtils.CcmStopForce(clusterInfo, 2);

                var t1 = new Thread(() =>
                {
                    Trace.TraceInformation("Thread started");
                    Trace.TraceInformation("[");
                    Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                    Trace.TraceInformation("]");

                    try
                    {
                        query(clusterInfo, 12);
                    }
                    catch (ThreadInterruptedException)
                    {
                        Trace.TraceInformation("Thread broke");
                        Trace.TraceInformation("[");
                        Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                        Trace.TraceInformation("]");
                    }
                    Trace.TraceInformation("Thread finished");
                    Trace.TraceInformation("[");
                    Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                    Trace.TraceInformation("]");
                });
                t1.Start();
                t1.Join(10000);
                if (t1.IsAlive)
                    t1.Interrupt();

                t1.Join();

                // A weak test to ensure that the nodes were contacted
                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                resetCoordinators();


                TestUtils.CcmStart(clusterInfo, 2);
                TestUtils.waitFor(IpPrefix + "2", clusterInfo.Cluster, 30);

                Trace.TraceInformation("MainThread started");
                Trace.TraceInformation("[");
                Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                Trace.TraceInformation("]");

                // Test successful reads
                for (int i = 0; i < 10; ++i)
                {
                    try
                    {
                        query(clusterInfo, 12);
                    }
                    catch (ThreadInterruptedException)
                    {
                        Trace.TraceInformation("Main Thread broke");
                        Trace.TraceInformation("[");
                        Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                        Trace.TraceInformation("]");
                    }
                }

                Trace.TraceInformation("Main Thread finished");
                Trace.TraceInformation("[");
                Trace.TraceInformation(Thread.CurrentThread.ManagedThreadId.ToString());
                Trace.TraceInformation("]");

                // A weak test to ensure that the nodes were contacted
                assertQueriedAtLeast(IpPrefix + "1", 1);
                assertQueriedAtLeast(IpPrefix + "2", 1);
                resetCoordinators();


                // Test writes
                for (int i = 0; i < 100; ++i)
                {
                    init(clusterInfo, 12);
                }

                // TODO: Missing test to see if nodes were written to


                // Test failed writes
                TestUtils.CcmStopForce(clusterInfo, 2);
                var t2 = new Thread(() =>
                {
                    Trace.TraceInformation("2 Thread started");
                    try
                    {
                        init(clusterInfo, 12);
                        Assert.Fail();
                    }
                    catch (ThreadInterruptedException)
                    {
                        Trace.TraceInformation("2 Thread async call broke");
                    }
                    catch (NoHostAvailableException)
                    {
                        Trace.TraceInformation("2 Thread no host");
                    }
                    Trace.TraceInformation("2 Thread finished");
                });
                t2.Start();
                t2.Join(10000);
                if (t2.IsAlive)
                    t2.Interrupt();

                t2.Join();

                // TODO: Missing test to see if nodes were written to
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        public class TestRetryPolicy : IRetryPolicy
        {
            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                               int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }
        }
    }
}