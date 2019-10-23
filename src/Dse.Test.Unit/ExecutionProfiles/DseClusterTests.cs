﻿// 
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
using Dse.ExecutionProfiles;
using Dse.Test.Unit.Connections;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.ExecutionProfiles
{
    [TestFixture]
    public class DseClusterTests
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
                Policies = new Dse.Policies(
                    lbps[0], 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    seps[0], 
                    new AtomicMonotonicTimestampGenerator()),
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
            var testDseConfig = new TestDseConfigurationBuilder(testConfig).Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);
            
            var dseCluster = new DseCluster(initializerMock, new List<string>(), testDseConfig, new DseCoreClusterFactory());
            dseCluster.Connect();
            
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
                Policies = new Dse.Policies(
                    lbp, 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    sep, 
                    new AtomicMonotonicTimestampGenerator())
            }.Build();
            var testDseConfig = new TestDseConfigurationBuilder(testConfig).Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);
            
            var dseCluster = new DseCluster(initializerMock, new List<string>(), testDseConfig, new DseCoreClusterFactory());
            dseCluster.Connect();

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
                Policies = new Dse.Policies(
                    lbps[0], 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    seps[0], 
                    new AtomicMonotonicTimestampGenerator()),
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
            var testDseConfig = new TestDseConfigurationBuilder(testConfig).Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);
            
            var dseCluster = new DseCluster(initializerMock, new List<string>(), testDseConfig, new DseCoreClusterFactory());
            dseCluster.Connect();
            dseCluster.Dispose();

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
                Policies = new Dse.Policies(
                    lbp, 
                    new ConstantReconnectionPolicy(50), 
                    new DefaultRetryPolicy(), 
                    sep, 
                    new AtomicMonotonicTimestampGenerator())
            }.Build();
            var testDseConfig = new TestDseConfigurationBuilder(testConfig).Build();
            var initializerMock = Mock.Of<IInitializer>();
            Mock.Get(initializerMock)
                .Setup(i => i.ContactPoints)
                .Returns(new List<IPEndPoint> { new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042) });
            Mock.Get(initializerMock)
                .Setup(i => i.GetConfiguration())
                .Returns(testConfig);
            
            var dseCluster = new DseCluster(initializerMock, new List<string>(), testDseConfig, new DseCoreClusterFactory());
            dseCluster.Connect();
            dseCluster.Dispose();

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