//
//      Copyright (C) 2012-2014 DataStax Inc.
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
        ///  Tests DowngradingConsistencyRetryPolicy
        /// </summary>
        public void DowngradingConsistencyRetryPolicyTest(Builder builder)
        {
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 3);

            // FIXME: Race condition where the nodes are not fully up yet and assertQueried reports slightly different numbers
            TestUtils.WaitForSchemaAgreement(clusterInfo);
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

                Thread.Sleep(60000);

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

                //A node takes at least 60 secs join the ring again
                Thread.Sleep(60000);

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
