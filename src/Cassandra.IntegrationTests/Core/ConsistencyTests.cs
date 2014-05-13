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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("long")]
    public class ConsistencyTests : PolicyTestTools
    {
        protected virtual string IpPrefix
        {
            get
            {
                return "127.0.0.";
            }
        }

        [Test]
        public void TestRFOneTokenAware()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 1);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.One);
                query(clusterInfo, 12, ConsistencyLevel.One);

                string assC = coordinators.First().Key.ToString();
                int awareCoord = int.Parse(assC.Substring(assC.Length - 1));

                assertQueried(IpPrefix + awareCoord, 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, awareCoord);
                TestUtils.waitForDownWithWait(IpPrefix + awareCoord, clusterInfo.Cluster, 30);

                var acceptedList = new List<ConsistencyLevel> {ConsistencyLevel.Any};

                var failList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.All
                };

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("It must not pass at consistency level {0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFTwoTokenAware()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 2);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.Two);
                query(clusterInfo, 12, ConsistencyLevel.Two);

                string assC = coordinators.First().Key.ToString();
                int awareCoord = int.Parse(assC.Substring(assC.Length - 1));

                assertQueried(IpPrefix + awareCoord, 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, awareCoord);
                TestUtils.waitForDownWithWait(IpPrefix + awareCoord, clusterInfo.Cluster, 30);

                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One
                };

                var failList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All
                };

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFThreeTokenAware()
        {
            Builder builder = Cluster.Builder().WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 3);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.Two);
                query(clusterInfo, 12, ConsistencyLevel.Two);

                string assC = coordinators.First().Key.ToString();
                int awareCoord = int.Parse(assC.Substring(assC.Length - 1));

                assertQueried(IpPrefix + awareCoord, 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, awareCoord);
                TestUtils.waitForDownWithWait(IpPrefix + awareCoord, clusterInfo.Cluster, 30);

                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum
                };

                var failList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All
                };

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFOneDowngradingCL()
        {
            Builder builder =
                Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 1);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.One);
                query(clusterInfo, 12, ConsistencyLevel.One);

                string assC = coordinators.First().Key.ToString();
                int awareCoord = int.Parse(assC.Substring(assC.Length - 1));

                assertQueried(IpPrefix + awareCoord, 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, awareCoord);
                TestUtils.waitForDownWithWait(IpPrefix + awareCoord, clusterInfo.Cluster, 30);

                var acceptedList = new List<ConsistencyLevel> {ConsistencyLevel.Any};

                var failList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.All
                };

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFTwoDowngradingCL()
        {
            Builder builder =
                Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 2);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.Two);
                query(clusterInfo, 12, ConsistencyLevel.Two);

                string assC = coordinators.First().Key.ToString();
                int awareCoord = int.Parse(assC.Substring(assC.Length - 1));

                assertQueried(IpPrefix + awareCoord, 12);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, awareCoord);
                TestUtils.waitForDownWithWait(IpPrefix + awareCoord, clusterInfo.Cluster, 30);

                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All
                };

                var failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFThreeRoundRobinDowngradingCL()
        {
            Builder builder =
                Cluster.Builder().WithLoadBalancingPolicy(new RoundRobinPolicy()).WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testRFThreeDowngradingCL(builder);
        }

        [Test]
        public void testRFThreeTokenAwareDowngradingCL()
        {
            Builder builder =
                Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            testRFThreeDowngradingCL(builder);
        }

        public void testRFThreeDowngradingCL(Builder builder)
        {
            var clusterInfo = TestUtils.CcmSetup(3, builder);
            createSchema(clusterInfo.Session, 3);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.All);
                query(clusterInfo, 12, ConsistencyLevel.All);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 2);
                TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);

                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All
                };

                var failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "consistency level EACH_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "consistency level LOCAL_QUORUM not compatible with replication strategy (org.apache.cassandra.locator.SimpleStrategy)",
                            "EACH_QUORUM ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFThreeDowngradingCLTwoDCs()
        {
            Builder builder =
                Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder, null, 3);
            createMultiDCSchema(clusterInfo.Session, 3, 3);
            //clusterInfo.Cluster.RefreshSchema();
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.Two);
                query(clusterInfo, 12, ConsistencyLevel.Two);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 12);
                assertQueried(IpPrefix + "4", 0);
                assertQueried(IpPrefix + "5", 0);
                assertQueried(IpPrefix + "6", 0);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 2);
                // FIXME: This sleep is needed to allow the waitFor() to work
                Thread.Sleep(20000);
                TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);

                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All,
                    ConsistencyLevel.LocalQuorum,
                    ConsistencyLevel.EachQuorum
                };

                var failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "EACH_QUORUM ConsistencyLevel is only supported for writes",
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                }
            }
            finally
            {
                resetCoordinators();
                TestUtils.CcmRemove(clusterInfo);
            }
        }

        [Test]
        public void testRFThreeDowngradingCLTwoDCsDCAware()
        {
            Builder builder =
                Cluster.Builder()
                       .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc2")))
                       .WithRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);
            var clusterInfo = TestUtils.CcmSetup(3, builder, null, 3);
            createMultiDCSchema(clusterInfo.Session, 3, 3);
            try
            {
                init(clusterInfo, 12, ConsistencyLevel.Two);
                query(clusterInfo, 12, ConsistencyLevel.Two);

                assertQueried(IpPrefix + "1", 0);
                assertQueried(IpPrefix + "2", 0);
                assertQueried(IpPrefix + "3", 0);
                // BUG: JAVA-88
                //assertQueried(IpPrefix + "4", 12);
                //assertQueried(IpPrefix + "5", 0);
                //assertQueried(IpPrefix + "6", 0);

                resetCoordinators();
                TestUtils.CcmStopForce(clusterInfo, 2);
                // FIXME: This sleep is needed to allow the waitFor() to work
                Thread.Sleep(20000);
                TestUtils.waitForDownWithWait(IpPrefix + "2", clusterInfo.Cluster, 5);


                var acceptedList = new List<ConsistencyLevel>
                {
                    ConsistencyLevel.Any,
                    ConsistencyLevel.One,
                    ConsistencyLevel.Two,
                    ConsistencyLevel.Quorum,
                    ConsistencyLevel.Three,
                    ConsistencyLevel.All,
                    ConsistencyLevel.LocalQuorum,
                    ConsistencyLevel.EachQuorum
                };

                var failList = new List<ConsistencyLevel>();

                // Test successful writes
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                    }
                    catch (Exception e)
                    {
                        Assert.Fail(String.Format("Test failed at CL.{0} with message: {1}", cl, e.Message));
                    }
                }

                // Test successful reads
                foreach (ConsistencyLevel cl in acceptedList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                    }
                    catch (InvalidQueryException e)
                    {
                        var acceptableErrorMessages = new List<string>
                        {
                            "EACH_QUORUM ConsistencyLevel is only supported for writes",
                            "ANY ConsistencyLevel is only supported for writes"
                        };
                        Assert.True(acceptableErrorMessages.Contains(e.Message), String.Format("Received: {0}", e.Message));
                    }
                }

                // Test writes which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        init(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
                    catch (WriteTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                }

                // Test reads which should fail
                foreach (ConsistencyLevel cl in failList)
                {
                    try
                    {
                        query(clusterInfo, 12, cl);
                        Assert.Fail(String.Format("Test passed at CL.{0}.", cl));
                    }
                    catch (ReadTimeoutException)
                    {
                        // expected to fail when the client hasn't marked the'
                        // node as DOWN yet
                    }
                    catch (UnavailableException)
                    {
                        // expected to fail when the client has already marked the
                        // node as DOWN
                    }
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