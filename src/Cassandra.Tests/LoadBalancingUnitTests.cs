using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Moq;
using System.Net;

namespace Cassandra.Tests
{
    [TestFixture]
    public class LoadBalancingUnitTests
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
            var firstRound = balancedHosts.Take(hostLength).ToList();
            foreach (var host in hostList)
            {
                //Check that each host appears only once
                Assert.AreEqual(1, firstRound.Where(h => h.Equals(host)).Count());
            }

            //test the same but in the following times
            var followingRounds = new List<Host>();
            for (var i = 0; i < 10; i++ )
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
            
            //Check that there arent remote nodes.
            Assert.AreEqual(0, followingRounds.Where(h => h.Datacenter != "local").Count());
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
