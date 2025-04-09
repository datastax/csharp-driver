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

using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.ExecutionProfiles;
using Cassandra.Tests.Connections.TestHelpers;
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
            var lbps = Enumerable.Range(1, 5).Select(i => new FakeLoadBalancingPolicy()).ToArray();
            var seps = Enumerable.Range(1, 5).Select(i => new FakeSpeculativeExecutionPolicy()).ToArray();
            var profile1 =
                new ExecutionProfileBuilder()
                                .WithSpeculativeExecutionPolicy(seps[1])
                                .WithLoadBalancingPolicy(lbps[1])
                                .CastToClass()
                                .Build();
            var testConfig = new TestConfigurationBuilder
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Cassandra.Policies(
                    lbps[0],
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    seps[0],
                    new AtomicMonotonicTimestampGenerator(),
                    null),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "profile1", profile1 },
                    {
                        "profile2",
                        new ExecutionProfileBuilder()
                                        .WithSpeculativeExecutionPolicy(seps[2])
                                        .CastToClass()
                                        .Build()
                    },
                    {
                        "profile3",
                        new ExecutionProfileBuilder()
                                        .WithLoadBalancingPolicy(lbps[2])
                                        .CastToClass()
                                        .Build()
                    },
                    {
                        "profile4",
                        new ExecutionProfileBuilder()
                                        .Build()
                    },
                    {
                        "profile5",
                        new ExecutionProfile(profile1, new ExecutionProfileBuilder().Build())
                    },
                    {
                        "graphProfile1",
                        new ExecutionProfileBuilder()
                            .WithLoadBalancingPolicy(lbps[4])
                            .WithSpeculativeExecutionPolicy(seps[4])
                            .CastToClass()
                            .Build()
                    },
                    {
                        "default",
                        new ExecutionProfileBuilder()
                            .WithLoadBalancingPolicy(lbps[3])
                            .WithSpeculativeExecutionPolicy(seps[3])
                            .CastToClass()
                            .Build()
                    }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();

            Assert.IsTrue(lbps.Skip(1).All(lbp => lbp.InitializeCount == 1));
            Assert.IsTrue(seps.Skip(1).All(sep => sep.InitializeCount == 1));
            Assert.AreEqual(0, lbps[0].InitializeCount);
            Assert.AreEqual(0, seps[0].InitializeCount);
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
                Policies = new Cassandra.Policies(
                    lbp,
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    sep,
                    new AtomicMonotonicTimestampGenerator(),
                    null)
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();

            Assert.AreEqual(1, lbp.InitializeCount);
            Assert.AreEqual(1, sep.InitializeCount);
        }

        [Test]
        public void Should_OnlyDisposePoliciesOnce_When_MultiplePoliciesAreProvidedWithExecutionProfiles()
        {
            var lbps = Enumerable.Range(1, 2).Select(i => new FakeLoadBalancingPolicy()).ToArray();
            var seps = Enumerable.Range(1, 4).Select(i => new FakeSpeculativeExecutionPolicy()).ToArray();
            var profile1 =
                new ExecutionProfileBuilder()
                                .WithSpeculativeExecutionPolicy(seps[1])
                                .WithLoadBalancingPolicy(lbps[1])
                                .CastToClass()
                                .Build();
            var testConfig = new TestConfigurationBuilder
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Cassandra.Policies(
                    lbps[0],
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    seps[0],
                    new AtomicMonotonicTimestampGenerator(),
                    null),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { "profile1", profile1 },
                    {
                        "profile2",
                        new ExecutionProfileBuilder()
                                        .WithSpeculativeExecutionPolicy(seps[1])
                                        .CastToClass()
                                        .Build()
                    },
                    {
                        "profile4",
                        new ExecutionProfileBuilder()
                                        .Build()
                    },
                    {
                        "profile5",
                        new ExecutionProfile(profile1, new ExecutionProfileBuilder().Build())
                    },
                    {
                        "graphProfile1",
                        new ExecutionProfileBuilder()
                            .WithSpeculativeExecutionPolicy(seps[3])
                            .CastToClass()
                            .Build() },
                    {
                        "default",
                        new ExecutionProfileBuilder()
                            .WithSpeculativeExecutionPolicy(seps[2])
                            .CastToClass()
                            .Build()
                    }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();
            cluster.Dispose();

            Assert.IsTrue(seps.Skip(1).All(sep => sep.DisposeCount == 1));
            Assert.AreEqual(0, seps[0].DisposeCount);
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
                Policies = new Cassandra.Policies(
                    lbp,
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    sep,
                    new AtomicMonotonicTimestampGenerator(),
                    null)
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();
            cluster.Dispose();

            Assert.AreEqual(1, sep.DisposeCount);
        }

        [Test]
        public void Should_OnlyDisposeRelevantPolicies_When_PoliciesAreProvidedByDefaultProfile()
        {
            var lbp1 = new FakeLoadBalancingPolicy();
            var sep1 = new FakeSpeculativeExecutionPolicy();
            var lbp2 = new FakeLoadBalancingPolicy();
            var sep2 = new FakeSpeculativeExecutionPolicy();
            var testConfig = new TestConfigurationBuilder()
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Cassandra.Policies(
                    lbp1,
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    sep1,
                    new AtomicMonotonicTimestampGenerator(),
                    null),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { Configuration.DefaultExecutionProfileName, new ExecutionProfile(null, null, null, lbp2, sep2, null, null) }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();
            cluster.Dispose();

            Assert.AreEqual(0, sep1.DisposeCount);
            Assert.AreEqual(1, sep2.DisposeCount);
        }

        [Test]
        public void Should_OnlyInitializeRelevantPolicies_When_PoliciesAreProvidedByDefaultProfile()
        {
            var lbp1 = new FakeLoadBalancingPolicy();
            var sep1 = new FakeSpeculativeExecutionPolicy();
            var lbp2 = new FakeLoadBalancingPolicy();
            var sep2 = new FakeSpeculativeExecutionPolicy();
            var testConfig = new TestConfigurationBuilder()
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                Policies = new Cassandra.Policies(
                    lbp1,
                    new ConstantReconnectionPolicy(50),
                    new DefaultRetryPolicy(),
                    sep1,
                    new AtomicMonotonicTimestampGenerator(),
                    null),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>
                {
                    { Configuration.DefaultExecutionProfileName, new ExecutionProfile(null, null, null, lbp2, sep2, null, null) }
                }
            }.Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);

            var cluster = Cluster.BuildFrom(initializerMock, new List<string>(), testConfig);
            cluster.Connect();

            Assert.AreEqual(0, lbp1.InitializeCount);
            Assert.AreEqual(0, sep1.InitializeCount);
            Assert.AreEqual(1, lbp2.InitializeCount);
            Assert.AreEqual(1, sep2.InitializeCount);
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