// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.ExecutionProfiles;
using Cassandra.Tests.Connections;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class ClusterTests
    {
        [Test]
        public void Should_OnlyInitializePoliciesOnce_When_MultiplePoliciesAreProvidedWithExecutionProfiles()
        {
            var lbps = Enumerable.Range(1, 4).Select(i => new FakeLoadBalancingPolicy()).ToArray();
            var seps = Enumerable.Range(1, 4).Select(i => new FakeSpeculativeExecutionPolicy()).ToArray();
            var profile1Builder = new ExecutionProfileBuilder();
            profile1Builder.
                WithSpeculativeExecutionPolicy(seps[1])
                .WithLoadBalancingPolicy(lbps[1]);
            var profile1 = profile1Builder.Build();

            var profile2Builder = new ExecutionProfileBuilder();
            profile2Builder.WithSpeculativeExecutionPolicy(seps[2]);
            var profile2 = profile2Builder.Build();

            var profile3Builder = new ExecutionProfileBuilder();
            profile3Builder.WithLoadBalancingPolicy(lbps[2]);
            var profile3 = profile3Builder.Build();

            var profile4 = new ExecutionProfileBuilder().Build();

            var profile5 = new ExecutionProfileBuilder().Build();

            var defaultProfileBuilder = new ExecutionProfileBuilder();
            defaultProfileBuilder
                .WithLoadBalancingPolicy(lbps[3])
                .WithSpeculativeExecutionPolicy(seps[3]);
            var defaultProfile = defaultProfileBuilder.Build();

            var testConfig = new TestConfigurationBuilder
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Policies(
                    lbps[0], 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    seps[0], 
                    new AtomicMonotonicTimestampGenerator()),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "profile1", profile1 },
                    { "profile2", profile2 },
                    { "profile3", profile3 },
                    { "profile4", profile4 },
                    { "profile5", new ExecutionProfile(profile1, profile5) },
                    { "default", defaultProfile }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            cluster.Connect();
            
            Assert.IsTrue(lbps.All(lbp => lbp.InitializeCount == 1));
            Assert.IsTrue(seps.All(sep => sep.InitializeCount == 1));
        }
        
        [Test]
        public void Should_OnlyInitializePoliciesOnce_When_NoProfileIsProvided()
        {
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var testConfig = new TestConfigurationBuilder()
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Policies(
                    lbp, 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    sep, 
                    new AtomicMonotonicTimestampGenerator())
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            cluster.Connect();

            Assert.AreEqual(1, lbp.InitializeCount);
            Assert.AreEqual(1, sep.InitializeCount);
        }
        
        [Test]
        public void Should_OnlyDisposePoliciesOnce_When_MultiplePoliciesAreProvidedWithExecutionProfiles()
        {
            var lbps = Enumerable.Range(1, 2).Select(i => new FakeLoadBalancingPolicy()).ToArray();
            var seps = Enumerable.Range(1, 3).Select(i => new FakeSpeculativeExecutionPolicy()).ToArray();
            var profile1Builder = new ExecutionProfileBuilder();
            profile1Builder
                .WithSpeculativeExecutionPolicy(seps[1])
                .WithLoadBalancingPolicy(lbps[1]);
            var profile1 = profile1Builder.Build();
            
            var profile2Builder = new ExecutionProfileBuilder();
            profile2Builder.WithSpeculativeExecutionPolicy(seps[1]);
            var profile2 = profile2Builder.Build();

            var defaultProfileBuilder = new ExecutionProfileBuilder();
            defaultProfileBuilder
                .WithSpeculativeExecutionPolicy(seps[2]);
            var defaultProfile = defaultProfileBuilder.Build();

            var testConfig = new TestConfigurationBuilder
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Policies(
                    lbps[0], 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    seps[0], 
                    new AtomicMonotonicTimestampGenerator()),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "profile1", profile1 },
                    { "profile2", profile2 },
                    { "profile4", new ExecutionProfileBuilder().Build() },
                    {
                        "profile5",
                        new ExecutionProfile(profile1, new ExecutionProfileBuilder().Build())
                    },
                    { "default", defaultProfile }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            cluster.Connect();
            cluster.Dispose();

            Assert.IsTrue(seps.All(sep => sep.DisposeCount == 1));
        }
        
        [Test]
        public void Should_OnlyDisposePoliciesOnce_When_NoProfileIsProvided()
        {
            var lbp = new FakeLoadBalancingPolicy();
            var sep = new FakeSpeculativeExecutionPolicy();
            var testConfig = new TestConfigurationBuilder()
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Policies(
                    lbp, 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    sep, 
                    new AtomicMonotonicTimestampGenerator())
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>());
            cluster.Connect();
            cluster.Dispose();

            Assert.AreEqual(1, sep.DisposeCount);
        }

        private class FakeSpeculativeExecutionPolicy : ISpeculativeExecutionPolicy
        {
            public volatile int InitializeCount;
            public volatile int DisposeCount;

            public void Dispose()
            {
                DisposeCount++;
            }

            public void Initialize(ICluster cluster)
            {
                InitializeCount++;
            }

            public ISpeculativeExecutionPlan NewPlan(string keyspace, IStatement statement)
            {
                throw new System.NotImplementedException();
            }
        }

        internal class FakeLoadBalancingPolicy : ILoadBalancingPolicy
        {
            public volatile int InitializeCount;
            private ICluster _cluster;

            public void Initialize(ICluster cluster)
            {
                _cluster = cluster;
                InitializeCount++;
            }

            public HostDistance Distance(Host host)
            {
                return HostDistance.Local;
            }

            public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
            {
                return _cluster.AllHosts();
            }
        }
    }
}