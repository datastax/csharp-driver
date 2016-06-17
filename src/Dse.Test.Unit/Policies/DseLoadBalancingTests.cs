//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra;
using Dse.Policies;
using NUnit.Framework;

namespace Dse.Test.Unit.Policies
{
    public class DseLoadBalancingTests : BaseUnitTest
    {
        [Test]
        public void Should_Yield_Preferred_Host_First()
        {
            var lbp = new DseLoadBalancingPolicy(new TestLoadBalancingPolicy());
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
            var lbp = new DseLoadBalancingPolicy(new TestLoadBalancingPolicy());
            var statement = new TargettedSimpleStatement("Q");
            var hosts = lbp.NewQueryPlan(null, statement);
            CollectionAssert.AreEqual(
                new[] { "101.0.0.0:9042", "102.0.0.0:9042" },
                hosts.Select(h => h.Address.ToString()));
        }
    }
}
