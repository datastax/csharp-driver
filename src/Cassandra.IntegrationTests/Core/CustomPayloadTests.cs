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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class CustomPayloadTests : TestGlobals
    {
        public ISession Session { get; set; }

        private const string Keyspace = "ks_custom_payload";
        private const string Table = Keyspace + ".tbl1";

        [OneTimeSetUp]
        public void SetupFixture()
        {
            if (TestClusterManager.CheckCassandraVersion(false, Version.Parse("2.2.0"), Comparison.LessThan))
            {
                Assert.Ignore("Requires Cassandra version >= 2.2");
                return;
            }

            //Using a mirroring handler, the server will reply providing the same payload that was sent
            var jvmArgs = new [] { "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler" };
            var testCluster = TestClusterManager.GetTestCluster(1, 0, false, DefaultMaxClusterCreateRetries, true, true, 0, jvmArgs);
            Session = testCluster.Session;
            Session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, Keyspace, 1));
            Session.Execute(string.Format(TestUtils.CreateTableSimpleFormat, Table));
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Query_Payload_Test()
        {
            var outgoing = new Dictionary<string, byte[]> { { "k1", Encoding.UTF8.GetBytes("value1") }, { "k2", Encoding.UTF8.GetBytes("value2") } };
            var stmt = new SimpleStatement("SELECT * FROM system.local");
            stmt.SetOutgoingPayload(outgoing);
            var rs = Session.Execute(stmt);
            Assert.NotNull(rs.Info.IncomingPayload);
            Assert.AreEqual(outgoing.Count, rs.Info.IncomingPayload.Count);
            CollectionAssert.AreEqual(outgoing["k1"], rs.Info.IncomingPayload["k1"]);
            CollectionAssert.AreEqual(outgoing["k2"], rs.Info.IncomingPayload["k2"]);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Batch_Payload_Test()
        {
            var outgoing = new Dictionary<string, byte[]> { { "k1-batch", Encoding.UTF8.GetBytes("value1") }, { "k2-batch", Encoding.UTF8.GetBytes("value2") } };
            var stmt = new BatchStatement();
            stmt.Add(new SimpleStatement(string.Format("INSERT INTO {0} (k, i) VALUES ('one', 1)", Table)));
            stmt.SetOutgoingPayload(outgoing);
            var rs = Session.Execute(stmt);
            Assert.NotNull(rs.Info.IncomingPayload);
            Assert.AreEqual(outgoing.Count, rs.Info.IncomingPayload.Count);
            CollectionAssert.AreEqual(outgoing["k1-batch"], rs.Info.IncomingPayload["k1-batch"]);
            CollectionAssert.AreEqual(outgoing["k2-batch"], rs.Info.IncomingPayload["k2-batch"]);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Bound_Payload_Test()
        {
            var outgoing = new Dictionary<string, byte[]> { { "k1-bound", Encoding.UTF8.GetBytes("value1") }, { "k2-bound", Encoding.UTF8.GetBytes("value2") } };
            var prepared = Session.Prepare("SELECT * FROM system.local WHERE key = ?");
            var stmt = prepared.Bind("local");
            stmt.SetOutgoingPayload(outgoing);
            var rs = Session.Execute(stmt);
            Assert.NotNull(rs.Info.IncomingPayload);
            Assert.AreEqual(outgoing.Count, rs.Info.IncomingPayload.Count);
            CollectionAssert.AreEqual(outgoing["k1-bound"], rs.Info.IncomingPayload["k1-bound"]);
            CollectionAssert.AreEqual(outgoing["k2-bound"], rs.Info.IncomingPayload["k2-bound"]);
        }

        [Test, TestCassandraVersion(2, 2)]
        public void Prepare_Payload_Test()
        {
            var outgoing = new Dictionary<string, byte[]>
            {
                { "k1-prep", Encoding.UTF8.GetBytes("value1-prep") }, 
                { "k2-prep", Encoding.UTF8.GetBytes("value2-prep") }
            };
            var prepared = Session.Prepare("SELECT key as k FROM system.local WHERE key = ?", outgoing);
            Assert.NotNull(prepared.IncomingPayload);
            Assert.AreEqual(outgoing.Count, prepared.IncomingPayload.Count);
            CollectionAssert.AreEqual(outgoing["k1-prep"], prepared.IncomingPayload["k1-prep"]);
            CollectionAssert.AreEqual(outgoing["k2-prep"], prepared.IncomingPayload["k2-prep"]);
        }
    }
}
