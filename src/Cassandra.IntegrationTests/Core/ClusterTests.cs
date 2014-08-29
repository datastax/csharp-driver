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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture]
    public class ClusterTests
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = System.Diagnostics.TraceLevel.Verbose;
        }

        [Test]
        public void ClusterAllHostsReturnsOnDisconnectedCluster()
        {
            const string ip = "127.100.100.100";
            var cluster = Cluster.Builder()
             .AddContactPoint(ip)
             .Build();
            //No ring was discovered
            Assert.AreEqual(1, cluster.AllHosts().Count);
            Assert.AreEqual(new IPEndPoint(IPAddress.Parse(ip), 9042), cluster.AllHosts().First().Address);
        }

        [Test]
        public void ClusterConnectThrowsNoHostAvailable()
        {
            var cluster = Cluster.Builder()
             .AddContactPoint("127.100.100.100")
             .Build();
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
            Assert.Throws<NoHostAvailableException>(() => cluster.Connect("sample_ks"));
        }
    }
}
