//
//      Copyright (C) DataStax Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra.Connections.Control;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections.TestHelpers;

using Moq;

using NUnit.Framework;

#pragma warning disable 618

namespace Cassandra.Tests
{
    [TestFixture]
    public class PoliciesUnitTests
    {
        [Test]
        public async Task RoundRobinIsCyclicTest()
        {
            byte hostLength = 4;
            var hostList = GetHostList(hostLength);

            var metadataMock = new Mock<IMetadata>();
            metadataMock
                .Setup(c => c.AllHostsSnapshot())
                .Returns(hostList)
                .Verifiable();
            var clusterMock = new Mock<ICluster>();
            clusterMock.SetupGet(c => c.Metadata).Returns(metadataMock.Object);

            //Initialize the balancing policy
            var policy = new RoundRobinPolicy();
            await policy.InitializeAsync(metadataMock.Object).ConfigureAwait(false);
            var balancedHosts = policy.NewQueryPlan(clusterMock.Object, null, new SimpleStatement());

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
                followingRounds.AddRange(policy.NewQueryPlan(clusterMock.Object, null, new SimpleStatement()));
            }
            Assert.AreEqual(10 * hostLength, followingRounds.Count);

            //Check that the cyclic order is correct
            for (var i = 1; i < followingRounds.Count - 2; i++)
            {
                Assert.AreNotSame(followingRounds[i - 1], followingRounds[i]);
                Assert.AreNotSame(followingRounds[i + 1], followingRounds[i]);
                Assert.AreNotSame(followingRounds[i + 2], followingRounds[i]);
            }

            metadataMock.Verify();
        }

        [Test]
        public async Task RoundRobinIsCyclicTestInParallel()
        {
            byte hostLength = 4;
            var hostList = GetHostList(hostLength);

            var metadataMock = new Mock<IMetadata>();
            metadataMock
                .Setup(c => c.AllHostsSnapshot())
                .Returns(hostList)
                .Verifiable();
            var clusterMock = new Mock<ICluster>();
            clusterMock.SetupGet(c => c.Metadata).Returns(metadataMock.Object);

            //Initialize the balancing policy
            var policy = new RoundRobinPolicy();
            await policy.InitializeAsync(metadataMock.Object).ConfigureAwait(false);

            Func<int, Task> action = async _ =>
            {
                var resultingHosts = new List<Host>();
                var hostEnumerator = policy.NewQueryPlan(clusterMock.Object, null, new SimpleStatement());
                foreach (var h in hostEnumerator)
                {
                    //Slow down to try to execute it at the same time
                    await Task.Delay(50).ConfigureAwait(false);
                    resultingHosts.Add(h);
                }
                Assert.AreEqual(hostLength, resultingHosts.Count);
                Assert.AreEqual(hostLength, resultingHosts.Distinct().Count());
            };

            var actions = Enumerable.Range(0, 100).Select(action);
            await Task.WhenAll(actions).ConfigureAwait(false);
            metadataMock.Verify();
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public async Task DcInferringPolicyInitializeInfersLocalDc(bool implicitContactPoint)
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList, implicitContactPoint: implicitContactPoint);
            var policy = new DcInferringLoadBalancingPolicy();
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);
            Assert.AreEqual(HostDistance.Local, policy.Distance(metadataMock.Cluster, hostList[0]));
            Assert.AreEqual(HostDistance.Remote, policy.Distance(metadataMock.Cluster, hostList[1]));
        }

        [Test]
        public async Task DCAwareRoundRobinPolicyNeverHitsRemote()
        {
            byte hostLength = 5;
            var hostList = new List<Host>();
            //Add a remote host at the beginning of the list
            hostList.AddRange(GetHostList(2, 1, "remote"));
            //add local hosts
            hostList.AddRange(GetHostList((byte)(hostLength - 2)));
            //Add another remote host at the end
            hostList.AddRange(GetHostList(2, 2, "remote"));

            var metadataMock = CreateMetadataMock(hostList);

            //Initialize the balancing policy
            //0 used nodes per remote dc
            var policy = new DCAwareRoundRobinPolicy("local");
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);
            var balancedHosts = policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement());
            var firstRound = balancedHosts.ToList();

            //Returns only local hosts
            Assert.AreEqual(hostLength - 2, firstRound.Count(h => h.Datacenter == "local"));
            Assert.AreEqual(0, firstRound.Count(h => h.Datacenter != "local"));

            //following rounds: test it multiple times
            var followingRounds = new List<Host>();
            for (var i = 0; i < 10; i++)
            {
                followingRounds.AddRange(policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement()).ToList());
            }
            Assert.AreEqual(10 * (hostLength - 2), followingRounds.Count);

            //Check that there aren't remote nodes.
            Assert.AreEqual(0, followingRounds.Count(h => h.Datacenter != "local"));
        }

        [Test]
        public async Task DCAwareRoundRobinInitializeUsesBuilderLocalDc()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList, "dc2");
            var policy = new DCAwareRoundRobinPolicy();
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);
            Assert.AreEqual(HostDistance.Remote, policy.Distance(metadataMock.Cluster, hostList[0]));
            Assert.AreEqual(HostDistance.Local, policy.Distance(metadataMock.Cluster, hostList[1]));
        }

        [Test]
        public void DCAwareRoundRobinInitializeDoesNotInferLocalDcWhenNotImplicitContactPoint()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList, implicitContactPoint: false);
            var policy = new DCAwareRoundRobinPolicy();
            var ex = Assert.ThrowsAsync<InvalidOperationException>(() => policy.InitializeAsync(metadataMock));
            Assert.AreEqual(
                "Since you provided explicit contact points, the local datacenter " +
                "must be explicitly set. It can be specified in the load balancing " +
                "policy constructor or via the Builder.WithLocalDatacenter() method." +
                " Available datacenters: dc1, dc2.",
                ex.Message);
        }

        [Test]
        public async Task DCAwareRoundRobinInitializeInfersLocalDcImplicitContactPoint()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList, implicitContactPoint: true);
            var policy = new DCAwareRoundRobinPolicy();
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);
            Assert.AreEqual(HostDistance.Local, policy.Distance(metadataMock.Cluster, hostList[0]));
            Assert.AreEqual(HostDistance.Remote, policy.Distance(metadataMock.Cluster, hostList[1]));
        }

        [Test]
        public void DCAwareRoundRobinInitializeNotMatchingDcThrows()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var clusterMock = CreateMetadataMock(hostList);
            var policy = new DCAwareRoundRobinPolicy("not_valid_dc");
            var ex = Assert.ThrowsAsync<ArgumentException>(() => policy.InitializeAsync(clusterMock));
            Assert.IsTrue(
                ex.Message.Contains("Datacenter not_valid_dc does not match any of the nodes, available datacenters:"),
                ex.Message);
        }

        [Test]
        public void DCAwareRoundRobinInitializeNotMatchingDcFromBuilderLocalDcThrows()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList, "not_valid_dc");
            var policy = new DCAwareRoundRobinPolicy();
            var ex = Assert.ThrowsAsync<ArgumentException>(() => policy.InitializeAsync(metadataMock));
            Assert.IsTrue(
                ex.Message.Contains("Datacenter not_valid_dc does not match any of the nodes, available datacenters:"),
                ex.Message);
        }

        [Test]
        public async Task DCAwareRoundRobinPolicyTestInParallel()
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

            var metadataMock = CreateMetadataMock(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc);
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);

            var allHosts = new ConcurrentBag<Host>();
            var firstHosts = new ConcurrentBag<Host>();
            Action action = () =>
            {
                var hosts = policy.NewQueryPlan(metadataMock.Cluster, null, null).ToList();
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
                    Assert.AreEqual(times / localHostsLength, firstHosts.Count(hc => hc == h));
                }
                else
                {
                    Assert.AreEqual(0, firstHosts.Count(hc => hc == h));
                }
            }
            Mock.Get(metadataMock.InternalMetadata).Verify();
        }

        [Test]
        public async Task DCAwareRoundRobinPolicyCachesLocalNodes()
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

            var metadataMock = CreateMetadataMock(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc);
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);

            var instances = new ConcurrentBag<object>();
            Action action = () => instances.Add(policy.GetHosts(metadataMock));
            TestHelper.ParallelInvoke(action, 100);
            Assert.AreEqual(1, instances.GroupBy(i => i.GetHashCode()).Count());
        }

        [Test]
        public async Task DCAwareRoundRobinPolicyWithNodesChanging()
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
            var metadataMock = CreateMetadataMock();
            Mock.Get(metadataMock.InternalMetadata)
                .Setup(c => c.AllHosts())
                .Returns(() =>
                {
                    return hostList.ToList();
                });

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy(localDc);
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);

            var hostYielded = new ConcurrentBag<IEnumerable<Host>>();
            Action action = () => hostYielded.Add(policy.NewQueryPlan(metadataMock.Cluster, null, null).ToList());

            //Invoke without nodes changing
            TestHelper.ParallelInvoke(action, 100);
            Assert.True(hostYielded.Any(hl => hl.Any(h => h == hostToRemove)));

            var actionList = new List<Action>(Enumerable.Repeat<Action>(action, 1000));

            actionList.Insert(200, () =>
            {
                var host = TestHelper.CreateHost("0.0.0.11", "dc1");
                //raise event and then add
                Mock.Get(metadataMock.InternalMetadata).Raise(c => c.HostAdded += null, host);
                hostList.Add(host);
            });
            actionList.Insert(400, () =>
            {
                var host = TestHelper.CreateHost("0.0.0.12", "dc1");
                //first add and then raise event
                hostList.Add(host);
                Mock.Get(metadataMock.InternalMetadata).Raise(c => c.HostAdded += null, host);
            });

            actionList.Insert(400, () =>
            {
                var host = hostToRemove;
                hostList = new ConcurrentBag<Host>(hostList.Where(h => h != hostToRemove));
                Mock.Get(metadataMock.InternalMetadata).Raise(c => c.HostRemoved += null, host);
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
            var policy = DowngradingConsistencyRetryPolicy.Instance.Wrap(Cassandra.Policies.DefaultExtendedRetryPolicy);
            var dummyStatement = new SimpleStatement().SetRetryPolicy(policy);
            //Retry if 1 of 2 replicas are alive
            var decision = RequestHandlerTests.GetRetryDecisionFromServerError(
                new UnavailableException(ConsistencyLevel.Two, 2, 1), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Retry if 2 of 3 replicas are alive
            decision = RequestHandlerTests.GetRetryDecisionFromServerError(
                new UnavailableException(ConsistencyLevel.Three, 3, 2), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Throw if 0 replicas are alive
            decision = RequestHandlerTests.GetRetryDecisionFromServerError(
                new UnavailableException(ConsistencyLevel.Three, 3, 0), policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Rethrow);

            //Retry if 1 of 3 replicas is alive
            decision =
                RequestHandlerTests.GetRetryDecisionFromServerError(new ReadTimeoutException(ConsistencyLevel.All, 3, 1, false),
                    policy, dummyStatement, config, 0);
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);
        }

        [Test]
        public void FixedReconnectionPolicyTests()
        {
            var delays = new long[] { 0, 2, 100, 200, 500, 1000 };
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
        public async Task TokenAwarePolicyReturnsLocalReplicasOnly()
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
            var metadataMock = CreateMetadataMock(hostList);
            Mock.Get(metadataMock.InternalMetadata)
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

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1"));
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);

            //key for host :::1 and :::3
            var k = new RoutingKey { RawRoutingKey = new byte[] { 1 } };
            var hosts = policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement().SetRoutingKey(k)).ToList();
            //5 local hosts
            Assert.AreEqual(5, hosts.Count);
            //local replica first
            Assert.AreEqual(1, TestHelper.GetLastAddressByte(hosts[0]));
            Mock.Get(metadataMock).Verify();

            //key for host :::2 and :::5
            k = new RoutingKey { RawRoutingKey = new byte[] { 2 } };
            n = 3;
            hosts = policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement().SetRoutingKey(k)).ToList();
            Assert.AreEqual(5, hosts.Count);
            //local replicas first
            CollectionAssert.AreEquivalent(new[] { 2, 5 }, hosts.Take(2).Select(TestHelper.GetLastAddressByte));
            //next should be local nodes
            Assert.AreEqual("dc1", hosts[2].Datacenter);
            Assert.AreEqual("dc1", hosts[3].Datacenter);
            Assert.AreEqual("dc1", hosts[4].Datacenter);
            Mock.Get(metadataMock.InternalMetadata).Verify();
        }

        [Test]
        public async Task TokenAwarePolicyRoundRobinsOnLocalReplicas()
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
            var metadataMock = CreateMetadataMock(hostList);
            Mock.Get(metadataMock.InternalMetadata)
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

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1"));
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);

            var firstHosts = new ConcurrentBag<Host>();
            var k = new RoutingKey { RawRoutingKey = new byte[] { 1 } };
            // key for host :::1 and :::2
            const int times = 10000;
            Action action = () =>
            {
                var h = policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement().SetRoutingKey(k)).First();
                firstHosts.Add(h);
            };
            TestHelper.ParallelInvoke(action, times);
            Assert.AreEqual(times, firstHosts.Count);
            double queryPlansWithHost1AsFirst = firstHosts.Count(h => TestHelper.GetLastAddressByte(h) == 1);
            double queryPlansWithHost2AsFirst = firstHosts.Count(h => TestHelper.GetLastAddressByte(h) == 2);
            Assert.AreEqual(times, queryPlansWithHost1AsFirst + queryPlansWithHost2AsFirst);
            // Around half will to one and half to the other
            Assert.That(queryPlansWithHost1AsFirst / times, Is.GreaterThan(0.45).And.LessThan(0.55));
            Assert.That(queryPlansWithHost2AsFirst / times, Is.GreaterThan(0.45).And.LessThan(0.55));
            Mock.Get(metadataMock.InternalMetadata).Verify();
        }

        [Test]
        public async Task TokenAwarePolicyReturnsChildHostsIfNoRoutingKey()
        {
            var hostList = new List<Host>
            {
                TestHelper.CreateHost("0.0.0.1", "dc1"),
                TestHelper.CreateHost("0.0.0.2", "dc1"),
                TestHelper.CreateHost("0.0.0.3", "dc2"),
                TestHelper.CreateHost("0.0.0.4", "dc2")
            };
            var metadataMock = CreateMetadataMock(hostList);

            var policy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy("dc1"));
            await policy.InitializeAsync(metadataMock).ConfigureAwait(false);
            //No routing key
            var hosts = policy.NewQueryPlan(metadataMock.Cluster, null, new SimpleStatement()).ToList();
            //2 localhosts
            Assert.AreEqual(2, hosts.Count(h => policy.Distance(metadataMock.Cluster, h) == HostDistance.Local));
            Assert.AreEqual("dc1", hosts[0].Datacenter);
            Assert.AreEqual("dc1", hosts[1].Datacenter);
            Mock.Get(metadataMock).Verify();
            //No statement
            hosts = policy.NewQueryPlan(metadataMock.Cluster, null, null).ToList();
            //2 localhosts
            Assert.AreEqual(2, hosts.Count(h => policy.Distance(metadataMock.Cluster, h) == HostDistance.Local));
            Assert.AreEqual("dc1", hosts[0].Datacenter);
            Assert.AreEqual("dc1", hosts[1].Datacenter);
            Mock.Get(metadataMock).Verify();
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

        private FakeMetadata CreateMetadataMock(
            ICollection<Host> hostList = null,
            string localDc = null,
            bool implicitContactPoint = false)
        {
            var config = new TestConfigurationBuilder { LocalDatacenter = localDc }.Build();
            var cluster = Mock.Of<IInternalCluster>();
            var internalMetadata = Mock.Of<IInternalMetadata>();
            var metadata = new FakeMetadata(cluster, internalMetadata);
            Mock.Get(cluster).SetupGet(c => c.Configuration).Returns(config);
            Mock.Get(cluster).SetupGet(c => c.InternalMetadata).Returns(internalMetadata);
            Mock.Get(cluster).SetupGet(c => c.Metadata).Returns(metadata);
            Mock.Get(cluster).SetupGet(c => c.ImplicitContactPoint).Returns(implicitContactPoint);
            if (hostList != null)
            {
                var cc = Mock.Of<IControlConnection>();
                Mock.Get(cc).SetupGet(c => c.Host).Returns(hostList.First());
                Mock.Get(internalMetadata).SetupGet(m => m.ControlConnection).Returns(cc);
                Mock.Get(internalMetadata).SetupGet(m => m.AllHosts()).Returns(hostList);
                config.LocalDatacenterProvider.Initialize(cluster, internalMetadata);
            }
            return metadata;
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