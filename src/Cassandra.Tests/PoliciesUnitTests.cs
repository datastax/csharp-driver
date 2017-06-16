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
﻿using System.Collections.Concurrent;
﻿using System.Collections.Generic;
using System.Linq;
﻿using System.Net;
﻿using NUnit.Framework;
using Moq;
using System.Threading.Tasks;
using System.Threading;
﻿using Cassandra.Requests;

namespace Cassandra.Tests
{
    [TestFixture]
    public class PoliciesUnitTests
    {
        [Test]
        public void RoundRobinIsCyclicTest()
        {
            byte hostLength = 4;
            var hostList = GetHostList(hostLength);

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList)
                .Verifiable();

            //Initialize the balancing policy
            var policy = new RoundRobinPolicy();
            policy.Initialize(clusterMock.Object);
            var balancedHosts = policy.NewQueryPlan(null, new SimpleStatement());

            //Take a list of hosts of 4, it should get 1 of every one in a cyclic order.
            var firstRound = balancedHosts.ToList();
            Assert.AreEqual(hostLength, firstRound.Count);
            foreach (var host in hostList)
            {
                //Check that each host appears only once
                Assert.AreEqual(1, firstRound.Where(h => h.Equals(host)).Count());
            }

            //test the same but in the following times
            var followingRounds = new List<Host>();
            for (var i = 0; i < 10; i++)
            {
                followingRounds.AddRange(policy.NewQueryPlan(null, new SimpleStatement()));
            }
            Assert.AreEqual(10 * hostLength, followingRounds.Count);

            //Check that the cyclic order is correct
            for (var i = 1; i < followingRounds.Count - 2; i++)
            {
                Assert.AreNotSame(followingRounds[i - 1], followingRounds[i]);
                Assert.AreNotSame(followingRounds[i + 1], followingRounds[i]);
                Assert.AreNotSame(followingRounds[i + 2], followingRounds[i]);
            }

            clusterMock.Verify();
        }

        [Test]
        public void RoundRobinIsCyclicTestInParallel()
        {
            byte hostLength = 4;
            var hostList = GetHostList(hostLength);

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList)
                .Verifiable();

            //Initialize the balancing policy
            var policy = new RoundRobinPolicy();
            policy.Initialize(clusterMock.Object);

            Action action = () =>
            {
                var resultingHosts = new List<Host>();
                var hostEnumerator = policy.NewQueryPlan(null, new SimpleStatement());
                foreach (var h in hostEnumerator)
                {
                    //Slow down to try to execute it at the same time
                    Thread.Sleep(50);
                    resultingHosts.Add(h);
                }
                Assert.AreEqual(hostLength, resultingHosts.Count);
                Assert.AreEqual(hostLength, resultingHosts.Distinct().Count());
            };

            var actions = new List<Action>();
            for (var i = 0; i < 100; i++)
            {
                actions.Add(action);
            }
            
            var parallelOptions = new ParallelOptions();
            parallelOptions.TaskScheduler = new ThreadPerTaskScheduler();
            parallelOptions.MaxDegreeOfParallelism = 1000;

            Parallel.Invoke(parallelOptions, actions.ToArray());
            clusterMock.Verify();
        }

        [Test]
        public void DCAwareRoundRobinPolicyNeverHitsRemoteWhenSet()
        {
            byte hostLength = 5;
            var hostList = new List<Host>();
            //Add a remote host at the beginning of the list
            hostList.AddRange(GetHostList(2, 1, "remote"));
            //add local hosts
            hostList.AddRange(GetHostList((byte)(hostLength - 2)));
            //Add another remote host at the end
            hostList.AddRange(GetHostList(2, 2, "remote"));

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);

            //Initialize the balancing policy
            //0 used nodes per remote dc
            var policy = new DCAwareRoundRobinPolicy("local", 0);
            policy.Initialize(clusterMock.Object);
            var balancedHosts = policy.NewQueryPlan(null, new SimpleStatement());
            var firstRound = balancedHosts.ToList();

            //Returns only local hosts
            Assert.AreEqual(hostLength - 2, firstRound.Count(h => h.Datacenter == "local"));
            Assert.AreEqual(0, firstRound.Count(h => h.Datacenter != "local"));

            //following rounds: test it multiple times
            var followingRounds = new List<Host>();
            for (var i = 0; i < 10; i++)
            {
                followingRounds.AddRange(policy.NewQueryPlan(null, new SimpleStatement()).ToList());
            }
            Assert.AreEqual(10 * (hostLength - 2), followingRounds.Count);
            
            //Check that there aren't remote nodes.
            Assert.AreEqual(0, followingRounds.Count(h => h.Datacenter != "local"));
        }

        [Test]
        public void DCAwareRoundRobinYieldsRemoteNodesAtTheEnd()
        {
            var hostList = new List<Host>
            {
                //5 local nodes and 4 remote
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2"),
                TestHelper.CreateHost("0.0.0.3", "dc1"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc2"),
                TestHelper.CreateHost("0.0.0.7", "dc1"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1"),
                TestHelper.CreateHost("0.0.0.10", "dc2")
            };
            var localHostsLength = hostList.Count(h => h.Datacenter == "dc1");
            const string localDc = "dc1";

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc, 1);
            policy.Initialize(clusterMock.Object);
            Action action = () =>
            {
                var hosts = policy.NewQueryPlan(null, null).ToList();
                for (var i = 0; i < hosts.Count; i++)
                {
                    var h = hosts[i];
                    if (i < localHostsLength)
                    {
                        Assert.AreEqual(localDc, h.Datacenter);
                    }
                    else
                    {
                        Assert.AreNotEqual(localDc, h.Datacenter);
                    }
                }
            };
            TestHelper.ParallelInvoke(action, 100);
        }

        [Test]
        public void DCAwareRoundRobinInitializeRetrievesLocalDc()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", null),
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);
            var policy = new DCAwareRoundRobinPolicy();
            policy.Initialize(clusterMock.Object);
            Assert.AreEqual(HostDistance.Local, policy.Distance(hostList[1]));
            Assert.AreNotEqual(HostDistance.Local, policy.Distance(hostList[2]));
        }

        [Test]
        public void DCAwareRoundRobinInitializeNotMatchingDcThrows()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);
            var policy = new DCAwareRoundRobinPolicy("not_valid_dc");
            Assert.Throws<ArgumentException>(() => policy.Initialize(clusterMock.Object));
        }

        [Test]
        public void DCAwareRoundRobinPolicyTestInParallel()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2"),
                TestHelper.CreateHost("0.0.0.3", "dc1"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc2"),
                TestHelper.CreateHost("0.0.0.7", "dc1"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1"),
                TestHelper.CreateHost("0.0.0.10", "dc2")
            };
            var localHostsLength = hostList.Count(h => h.Datacenter == "dc1");
            const string localDc = "dc1";

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc, 1);
            policy.Initialize(clusterMock.Object);

            var allHosts = new ConcurrentBag<Host>();
            var firstHosts = new ConcurrentBag<Host>();
            Action action = () =>
            {
                var hosts = policy.NewQueryPlan(null, null).ToList();
                //Check that the value is not repeated
                Assert.AreEqual(0, hosts.GroupBy(x => x)
                    .Where(g => g.Count() > 1)
                    .Select(y => y.Key)
                    .Count());
                firstHosts.Add(hosts[0]);
                //Add to the general list
                foreach (var h in hosts)
                {
                    allHosts.Add(h);
                }
            };

            var actions = new List<Action>();
            const int times = 100;
            for (var i = 0; i < times; i++)
            {
                actions.Add(action);
            }
            TestHelper.ParallelInvoke(actions);

            //Check that the first nodes where different
            foreach (var h in hostList)
            {
                if (h.Datacenter == localDc)
                {
                    Assert.AreEqual(times/localHostsLength, firstHosts.Count(hc => hc == h));
                }
                else
                {
                    Assert.AreEqual(0, firstHosts.Count(hc => hc == h));
                }
            }
            clusterMock.Verify();
        }

        [Test]
        public void DCAwareRoundRobinPolicyCachesLocalNodes()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2"),
                TestHelper.CreateHost("0.0.0.3", "dc1"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc2"),
                TestHelper.CreateHost("0.0.0.7", "dc1"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1"),
                TestHelper.CreateHost("0.0.0.10", "dc2")
            };
            const string localDc = "dc1";

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc, 1);
            policy.Initialize(clusterMock.Object);

            var instances = new ConcurrentBag<object>();
            Action action = () => instances.Add(policy.GetHosts());
            TestHelper.ParallelInvoke(action, 100);
            Assert.AreEqual(1, instances.GroupBy(i => i.GetHashCode()).Count());
        }

        [Test]
        public void DCAwareRoundRobinPolicyWithNodesChanging()
        {
            var hostList = new ConcurrentBag<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2"),
                TestHelper.CreateHost("0.0.0.3", "dc1"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc2"),
                TestHelper.CreateHost("0.0.0.7", "dc1"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1"),
                TestHelper.CreateHost("0.0.0.10", "dc2")
            };
            const string localDc = "dc1";
            //to remove the host 3
            var hostToRemove = hostList.First(h => TestHelper.GetLastAddressByte(h) == 3);
            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(() =>
                {
                    return hostList.ToList();
                });
            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc, 1);
            policy.Initialize(clusterMock.Object);

            var hostYielded = new ConcurrentBag<IEnumerable<Host>>();
            Action action = () => hostYielded.Add(policy.NewQueryPlan(null, null).ToList());

            //Invoke without nodes changing
            TestHelper.ParallelInvoke(action, 100);
            Assert.True(hostYielded.Any(hl => hl.Any(h => h == hostToRemove)));

            var actionList = new List<Action>(Enumerable.Repeat<Action>(action, 1000));


            actionList.Insert(200, () =>
            {
                var host = TestHelper.CreateHost("0.0.0.11", "dc1");
                //raise event and then add
                clusterMock.Raise(c => c.HostAdded += null, host);
                hostList.Add(host);
            });
            actionList.Insert(400, () =>
            {
                var host = TestHelper.CreateHost("0.0.0.12", "dc1");
                //first add and then raise event
                hostList.Add(host);
                clusterMock.Raise(c => c.HostAdded += null, host);
            });
            
            actionList.Insert(400, () =>
            {
                var host = hostToRemove;
                hostList = new ConcurrentBag<Host>(hostList.Where(h => h != hostToRemove));
                clusterMock.Raise(c => c.HostRemoved += null, host);
            });

            //Invoke it with nodes being modified
            TestHelper.ParallelInvoke(actionList);
            //Clear the host yielded so far
            hostYielded = new ConcurrentBag<IEnumerable<Host>>();
            //Invoke it a some of times more in parallel
            TestHelper.ParallelInvoke(action, 100);
            //The removed node should not be returned
            Assert.False(hostList.Any(h => h == hostToRemove));
            Assert.False(hostYielded.Any(hl => hl.Any(h => h == hostToRemove)));
        }

        /// <summary>
        /// Unit test on retry decisions
        /// </summary>
        [Test]
        public void DowngradingConsistencyRetryTest()
        {
            var config = new Configuration();
            var policy = DowngradingConsistencyRetryPolicy.Instance.Wrap(null);
            var dummyStatement = new SimpleStatement().SetRetryPolicy(policy);
            //Retry if 1 of 2 replicas are alive
            var decision = RequestExecution<RowSet>.GetRetryDecision(
                new UnavailableException(ConsistencyLevel.Two, 2, 1), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Retry if 2 of 3 replicas are alive
            decision = RequestExecution<RowSet>.GetRetryDecision(
                new UnavailableException(ConsistencyLevel.Three, 3, 2), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Throw if 0 replicas are alive
            decision = RequestExecution<RowSet>.GetRetryDecision(
                new UnavailableException(ConsistencyLevel.Three, 3, 0), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Rethrow);

            //Retry if 1 of 3 replicas is alive
            decision =
                RequestExecution<RowSet>.GetRetryDecision(new ReadTimeoutException(ConsistencyLevel.All, 3, 1, false),
                    policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);
        }

        [Test]
        public void FixedReconnectionPolicyTests()
        {
            var delays = new long[] {0, 2, 100, 200, 500, 1000};
            var policy = new FixedReconnectionPolicy(delays);
            var schedule = policy.NewSchedule();
            const int times = 30;
            var actualDelays = new List<long>();
            for (var i = 0; i < times; i++)
            {
                actualDelays.Add(schedule.NextDelayMs());
            }
            //The last delay will be used for the rest.
            //Add the n times the last delay (1000)
            var expectedDelays = delays.Concat(Enumerable.Repeat<long>(1000, times - delays.Length));
            Assert.AreEqual(expectedDelays, actualDelays);
        }

        [Test]
        public void TokenAwarePolicyReturnsLocalReplicasFirst()
        {
            var hostList = new List<Host>
            {
                //5 local nodes and 4 remote
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc1"),
                TestHelper.CreateHost("0.0.0.3", "dc2"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc1"),
                TestHelper.CreateHost("0.0.0.7", "dc2"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1")
            };
            var n = 2;
            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList)
                .Verifiable();
            clusterMock
                .Setup(c => c.GetReplicas(It.IsAny<string>(), It.IsAny<byte[]>()))
                .Returns<string, byte[]>((keyspace, key) =>
                {
                    var i = key[0];
                    return hostList.Where(h =>
                    {
                        //The host at with address == k || address == k + n
                        var address = TestHelper.GetLastAddressByte(h);
                        return address == i || address == i + n;
                    }).ToList();
                })
                .Verifiable();

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1", 2));
            policy.Initialize(clusterMock.Object);

            //key for host :::1 and :::3
            var k = new RoutingKey { RawRoutingKey = new byte[] { 1 } };
            var hosts = policy.NewQueryPlan(null, new SimpleStatement().SetRoutingKey(k)).ToList();
            //5 local hosts + 2 remote hosts
            Assert.AreEqual(7, hosts.Count);
            //local replica first
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(hosts[0]));
            clusterMock.Verify();

            //key for host :::2 and :::5
            k = new RoutingKey { RawRoutingKey = new byte[] { 2 } };
            n = 3;
            hosts = policy.NewQueryPlan(null, new SimpleStatement().SetRoutingKey(k)).ToList();
            Assert.AreEqual(7, hosts.Count);
            //local replicas first
            CollectionAssert.AreEquivalent(new[] { 2, 5}, hosts.Take(2).Select(TestHelper.GetLastAddressByte));
            //next should be local nodes
            Assert.AreEqual("dc1", hosts[2].Datacenter);
            Assert.AreEqual("dc1", hosts[3].Datacenter);
            Assert.AreEqual("dc1", hosts[4].Datacenter);
            clusterMock.Verify();
        }

        [Test]
        public void TokenAwarePolicyRoundRobinsOnLocalReplicas()
        {
            var hostList = new List<Host>
            {
                //5 local nodes and 4 remote
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc1"),
                TestHelper.CreateHost("0.0.0.3", "dc2"),
                TestHelper.CreateHost("0.0.0.4", "dc2"),
                TestHelper.CreateHost("0.0.0.5", "dc1"),
                TestHelper.CreateHost("0.0.0.6", "dc1"),
                TestHelper.CreateHost("0.0.0.7", "dc2"),
                TestHelper.CreateHost("0.0.0.8", "dc2"),
                TestHelper.CreateHost("0.0.0.9", "dc1")
            };
            var clusterMock = new Mock<ICluster>(MockBehavior.Strict);
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList)
                .Verifiable();
            clusterMock
                .Setup(c => c.GetReplicas(It.IsAny<string>(), It.IsAny<byte[]>()))
                .Returns<string, byte[]>((keyspace, key) =>
                {
                    var i = key[0];
                    return hostList.Where(h =>
                    {
                        //The host at with address == k and the next one
                        var address = TestHelper.GetLastAddressByte(h);
                        return address == i || address == i + 1;
                    }).ToList();
                })
                .Verifiable();

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1", 2));
            policy.Initialize(clusterMock.Object);

            var firstHosts = new ConcurrentBag<Host>();
            var k = new RoutingKey { RawRoutingKey = new byte[] { 1 } };
            // key for host :::1 and :::2
            const int times = 10000;
            Action action = () =>
            {
                var h = policy.NewQueryPlan(null, new SimpleStatement().SetRoutingKey(k)).First();
                firstHosts.Add(h);
            };
            TestHelper.ParallelInvoke(action, times);
            Assert.AreEqual(times, firstHosts.Count);
            double queryPlansWithHost1AsFirst = firstHosts.Count(h => TestHelper.GetLastAddressByte(h) == 1);
            double queryPlansWithHost2AsFirst = firstHosts.Count(h => TestHelper.GetLastAddressByte(h) == 2);
            Assert.AreEqual(times, queryPlansWithHost1AsFirst + queryPlansWithHost2AsFirst);
            // Around half will to one and half to the other
            Assert.That(queryPlansWithHost1AsFirst / times, Is.GreaterThan(0.48).And.LessThan(0.52));
            Assert.That(queryPlansWithHost2AsFirst / times, Is.GreaterThan(0.48).And.LessThan(0.52));
            clusterMock.Verify();
        }

        [Test]
        public void TokenAwarePolicyReturnsChildHostsIfNoRoutingKey()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc1"),
                TestHelper.CreateHost("0.0.0.3", "dc2"),
                TestHelper.CreateHost("0.0.0.4", "dc2")
            };
            var clusterMock = new Mock<ICluster>(MockBehavior.Strict);
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList)
                .Verifiable();

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1", 1));
            policy.Initialize(clusterMock.Object);
            //No routing key
            var hosts = policy.NewQueryPlan(null, new SimpleStatement()).ToList();
            //2 localhosts
            Assert.AreEqual(2, hosts.Count(h => policy.Distance(h) == HostDistance.Local));
            Assert.AreEqual("dc1", hosts[0].Datacenter);
            Assert.AreEqual("dc1", hosts[1].Datacenter);
            clusterMock.Verify();
            //No statement
            hosts = policy.NewQueryPlan(null, null).ToList();
            //2 localhosts
            Assert.AreEqual(2, hosts.Count(h => policy.Distance(h) == HostDistance.Local));
            Assert.AreEqual("dc1", hosts[0].Datacenter);
            Assert.AreEqual("dc1", hosts[1].Datacenter);
            clusterMock.Verify();
        }

        [Test]
        public void IdempotenceAwareRetryPolicy_Should_Use_ChildPolicy_OnReadTimeout()
        {
            var testPolicy = new TestRetryPolicy();
            var policy = new IdempotenceAwareRetryPolicy(testPolicy);
            var decision = policy.OnReadTimeout(new SimpleStatement("Q"), ConsistencyLevel.All, 0, 0, true, 1);
            Assert.AreEqual(decision.DecisionType, RetryDecision.RetryDecisionType.Ignore);
            Assert.AreEqual(1, testPolicy.ReadTimeoutCounter);
            Assert.AreEqual(0, testPolicy.WriteTimeoutCounter);
            Assert.AreEqual(0, testPolicy.UnavailableCounter);
        }

        [Test]
        public void IdempotenceAwareRetryPolicy_Should_Use_ChildPolicy_OnUnavailable()
        {
            var testPolicy = new TestRetryPolicy();
            var policy = new IdempotenceAwareRetryPolicy(testPolicy);
            var decision = policy.OnUnavailable(new SimpleStatement("Q"), ConsistencyLevel.All, 0, 0, 1);
            Assert.AreEqual(decision.DecisionType, RetryDecision.RetryDecisionType.Ignore);
            Assert.AreEqual(0, testPolicy.ReadTimeoutCounter);
            Assert.AreEqual(0, testPolicy.WriteTimeoutCounter);
            Assert.AreEqual(1, testPolicy.UnavailableCounter);
        }

        [Test]
        public void IdempotenceAwareRetryPolicy_Should_Use_ChildPolicy_OnWriteTimeout_With_Idempotent_Statements()
        {
            var testPolicy = new TestRetryPolicy();
            var policy = new IdempotenceAwareRetryPolicy(testPolicy);
            var decision = policy.OnWriteTimeout(new SimpleStatement("Q").SetIdempotence(true), ConsistencyLevel.All, "BATCH", 0, 0, 1);
            Assert.AreEqual(decision.DecisionType, RetryDecision.RetryDecisionType.Ignore);
            Assert.AreEqual(0, testPolicy.ReadTimeoutCounter);
            Assert.AreEqual(1, testPolicy.WriteTimeoutCounter);
            Assert.AreEqual(0, testPolicy.UnavailableCounter);
        }

        [Test]
        public void IdempotenceAwareRetryPolicy_Should_Rethrow_OnWriteTimeout_With_Non_Idempotent_Statements()
        {
            var testPolicy = new TestRetryPolicy();
            var policy = new IdempotenceAwareRetryPolicy(testPolicy);
            var decision = policy.OnWriteTimeout(new SimpleStatement("Q").SetIdempotence(false), ConsistencyLevel.All, "BATCH", 0, 0, 1);
            Assert.AreEqual(decision.DecisionType, RetryDecision.RetryDecisionType.Rethrow);
            Assert.AreEqual(0, testPolicy.ReadTimeoutCounter);
            Assert.AreEqual(0, testPolicy.WriteTimeoutCounter);
            Assert.AreEqual(0, testPolicy.UnavailableCounter);
        }

        /// <summary>
        /// Creates a list of host with ips starting at 0.0.0.0 to 0.0.0.(length-1) and the provided datacenter name
        /// </summary>
        private static List<Host> GetHostList(byte length, byte thirdPosition = 0, string datacenter = "local")
        {
            var list = new List<Host>();
            for (byte i = 0; i < length; i++)
            {
                var host = TestHelper.CreateHost("0.0." + thirdPosition + "." + i, datacenter);
                list.Add(host);
            }
            return list;
        }

        private class TestRetryPolicy : IRetryPolicy
        {
            public int ReadTimeoutCounter { get; set; }

            public int WriteTimeoutCounter { get; set; }

            public int UnavailableCounter { get; set; }

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                ReadTimeoutCounter++;
                return RetryDecision.Ignore();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                WriteTimeoutCounter++;
                return RetryDecision.Ignore();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                UnavailableCounter++;
                return RetryDecision.Ignore();
            }
        }
    }
}