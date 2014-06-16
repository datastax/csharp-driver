﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Moq;
using System.Net;
using System.Threading.Tasks;
using System.Threading;

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
            var balancedHosts = policy.NewQueryPlan(new SimpleStatement());

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
                followingRounds.AddRange(policy.NewQueryPlan(new SimpleStatement()));
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
                var hostEnumerator = policy.NewQueryPlan(new SimpleStatement());
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
        public void DCAwareRoundRobinPolicyNeverHistsOtherDcWhenNodeUp()
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
            var policy = new DCAwareRoundRobinPolicy("local");
            policy.Initialize(clusterMock.Object);
            var balancedHosts = policy.NewQueryPlan(new SimpleStatement());
            var firstRound = balancedHosts.Take(hostLength - 2).ToList();

            //Returns only local hosts
            Assert.AreEqual(hostLength - 2, firstRound.Where(h => h.Datacenter == "local").Count());
            Assert.AreEqual(0, firstRound.Where(h => h.Datacenter != "local").Count());

            //following rounds: test it multiple times
            var followingRounds = new List<Host>();
            for (var i = 0; i < 10; i++)
            {
                followingRounds.AddRange(policy.NewQueryPlan(new SimpleStatement()).Take(hostLength - 2));
            }
            Assert.AreEqual(10 * (hostLength - 2), followingRounds.Count);
            
            //Check that there aren't remote nodes.
            Assert.AreEqual(0, followingRounds.Where(h => h.Datacenter != "local").Count());
        }

        [Test]
        public void DCAwareRoundRobinPolicyTestInParallel()
        {
            byte hostLength = 6;
            var hostList = new List<Host>();
            hostList.AddRange(GetHostList(1, 1, "remote1"));
            hostList.AddRange(GetHostList(1, 1, "remote2"));
            hostList.AddRange(GetHostList((byte)(hostLength - 3)));
            hostList.AddRange(GetHostList(1, 2, "remote1"));

            var clusterMock = new Mock<ICluster>();
            clusterMock
                .Setup(c => c.AllHosts())
                .Returns(hostList);

            //Initialize the balancing policy
            var policy = new DCAwareRoundRobinPolicy("local", 1);
            policy.Initialize(clusterMock.Object);

            Action action = () =>
            {
                var resultingHosts = new List<Host>();
                var hostEnumerator = policy.NewQueryPlan(new SimpleStatement());
                foreach (var h in hostEnumerator)
                {
                    //Slow down to try to execute it at the same time
                    Thread.Sleep(50);
                    resultingHosts.Add(h);
                }
                //The first hosts should be local
                Assert.True(resultingHosts.Take(hostLength-3).All(h => h.Datacenter == "local"));
                //It should return the local hosts first and then 1 per each dc
                Assert.AreEqual(hostLength - 1, resultingHosts.Count);
                Assert.AreEqual(hostLength - 1, resultingHosts.Distinct().Count());
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

        /// <summary>
        /// Unit test on retry decisions
        /// </summary>
        [Test]
        public void DowngradingConsistencyRetryTest()
        {
            var dummyStatement = new SimpleStatement().SetRetryPolicy(DowngradingConsistencyRetryPolicy.Instance);

            var handler = new RequestHandler<RowSet>(null, null, dummyStatement);
            //Retry if 1 of 2 replicas are alive
            var decision = handler.GetRetryDecision(new UnavailableException(ConsistencyLevel.Two, 2, 1));
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Retry if 2 of 3 replicas are alive
            decision = handler.GetRetryDecision(new UnavailableException(ConsistencyLevel.Three, 3, 2));
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);

            //Throw if 0 replicas are alive
            decision = handler.GetRetryDecision(new UnavailableException(ConsistencyLevel.Three, 3, 0));
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Rethrow);

            //Retry if 1 of 3 replicas is alive
            decision = handler.GetRetryDecision(new ReadTimeoutException(ConsistencyLevel.All, 3, 1, false));
            Assert.True(decision != null && decision.DecisionType == RetryDecision.RetryDecisionType.Retry);
        }

        /// <summary>
        /// Creates a list of host with ips starting at 0.0.0.0 to 0.0.0.(length-1) and the provided datacenter name
        /// </summary>
        private List<Host> GetHostList(byte length, byte thirdPosition = 0, string datacenter = "local")
        {
            var list = new List<Host>();
            for (byte i = 0; i < length; i++)
            {
                var host = new Host(new IPAddress(new byte[] { 0, 0, thirdPosition, i }), new ConstantReconnectionPolicy(100));
                host.SetLocationInfo(datacenter, "rack1");
                list.Add(host);
            }
            return list;
        }
    }
}
