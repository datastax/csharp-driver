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

using Cassandra.ExecutionProfiles;
using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class ExecutionProfileBuilderTests
    {
        [Test]
        public void Should_GetAllSettingsFromBaseProfile_When_DerivedProfileHasNoSettings()
        { 
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var baseProfileBuilder = new ExecutionProfileBuilder();
            baseProfileBuilder
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                .WithConsistencyLevel(ConsistencyLevel.Quorum)
                .WithReadTimeoutMillis(3000)
                .WithRetryPolicy(rp);

            var baseProfile = baseProfileBuilder.Build();

            var profile = new ExecutionProfile(baseProfile, new ExecutionProfileBuilder().Build());

            Assert.AreSame(lbp, profile.LoadBalancingPolicy);
            Assert.AreSame(sep, profile.SpeculativeExecutionPolicy);
            Assert.AreSame(rp, profile.RetryPolicy);
            Assert.AreEqual(3000, profile.ReadTimeoutMillis);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, profile.SerialConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.Quorum, profile.ConsistencyLevel);
        }
        
        [Test]
        public void Should_GetNoSettingFromBaseProfile_When_DerivedProfileHasAllSettings()
        { 
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var lbpProfile = new TokenAwarePolicy(new DCAwareRoundRobinPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var baseProfileBuilder = new ExecutionProfileBuilder();
            baseProfileBuilder
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                .WithConsistencyLevel(ConsistencyLevel.Quorum)
                .WithReadTimeoutMillis(3000)
                .WithRetryPolicy(rp);

            var baseProfile = baseProfileBuilder.Build();
            
            var derivedProfileBuilder = new ExecutionProfileBuilder();
            derivedProfileBuilder
                .WithLoadBalancingPolicy(lbpProfile)
                .WithSpeculativeExecutionPolicy(sepProfile)
                .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                .WithReadTimeoutMillis(5000)
                .WithRetryPolicy(rpProfile);

            var derivedProfile = derivedProfileBuilder.Build();
            
            var profile = new ExecutionProfile(baseProfile, derivedProfile);

            Assert.AreSame(lbpProfile, profile.LoadBalancingPolicy);
            Assert.AreSame(sepProfile, profile.SpeculativeExecutionPolicy);
            Assert.AreSame(rpProfile, profile.RetryPolicy);
            Assert.AreEqual(5000, profile.ReadTimeoutMillis);
            Assert.AreEqual(ConsistencyLevel.Serial, profile.SerialConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalQuorum, profile.ConsistencyLevel);
        }
    }
}