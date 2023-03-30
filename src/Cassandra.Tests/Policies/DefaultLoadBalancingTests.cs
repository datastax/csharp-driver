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
using System.Linq;
using System.Net;
using NUnit.Framework;

namespace Cassandra.Tests.Policies
{
    public class DefaultLoadBalancingTests : BaseUnitTest
    {
        [Test]
        public void Should_Yield_Preferred_Host_First()
        {
#pragma warning disable 618
            var lbp = new DefaultLoadBalancingPolicy(new TestLoadBalancingPolicy());
#pragma warning restore 618
            var statement = new TargettedSimpleStatement("Q");
            statement.PreferredHost = new Host(new IPEndPoint(201, 9042), ReconnectionPolicy);
            var hosts = lbp.NewQueryPlan(null, statement);
            CollectionAssert.AreEqual(
                new[] { "201.0.0.0:9042", "101.0.0.0:9042", "102.0.0.0:9042" }, 
                hosts.Select(h => h.Address.ToString()));
        }

        [Test]
        public void Should_Yield_Child_Hosts_When_No_Preferred_Host_Defined()
        {
#pragma warning disable 618
            var lbp = new DefaultLoadBalancingPolicy(new TestLoadBalancingPolicy());
#pragma warning restore 618
            var statement = new TargettedSimpleStatement("Q");
            var hosts = lbp.NewQueryPlan(null, statement);
            CollectionAssert.AreEqual(
                new[] { "101.0.0.0:9042", "102.0.0.0:9042" },
                hosts.Select(h => h.Address.ToString()));
        }

        [Test]
        public void Should_Set_Distance_For_Preferred_Host_To_Local()
        {
#pragma warning disable 618
            var lbp = new DefaultLoadBalancingPolicy(new TestLoadBalancingPolicy(HostDistance.Ignored));
#pragma warning restore 618
            Assert.AreEqual(HostDistance.Ignored, lbp.Distance(new Host(new IPEndPoint(200L, 9042), ReconnectionPolicy)));
            var statement = new TargettedSimpleStatement("Q");
            // Use 201 as preferred
            statement.PreferredHost = new Host(new IPEndPoint(201L, 9042), ReconnectionPolicy);
            lbp.NewQueryPlan(null, statement);
            Assert.AreEqual(HostDistance.Local, lbp.Distance(statement.PreferredHost));
        }
    }
}
