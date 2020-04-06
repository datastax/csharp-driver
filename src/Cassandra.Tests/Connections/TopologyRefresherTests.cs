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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ProtocolEvents;
using Cassandra.Responses;
using Cassandra.SessionManagement;
using Cassandra.Tests.DataStax.Insights;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class TopologyRefresherTests
    {
        [Test]
        public void Should_SetClusterName()
        {
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = Mock.Of<IMetadataRequestHandler>()
            }.Build();
            var metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(metadata, config);
            var row = TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, { "data_center", "ut-dc" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1-SNAPSHOT"}
            });
            var connection = Mock.Of<IConnection>();
            var response = new FakeResultResponse(ResultResponse.ResultResponseKind.Rows);
            Mock.Get(config.MetadataRequestHandler).Setup(c => c.SendMetadataRequestAsync(
                    It.IsAny<IConnection>(), ProtocolVersion.MaxSupported, "SELECT * FROM system.local WHERE key='local'", QueryProtocolOptions.Default))
                .ReturnsAsync(new FakeResultResponse(ResultResponse.ResultResponseKind.Rows));
            Mock.Get(config.MetadataRequestHandler).Setup(c => c.GetRowSet(response))
                .Returns(new List<Row> { row });

            var _ = topologyRefresher.RefreshNodeList(new FakeConnectionEndPoint("127.0.0.1", 9042), connection, ProtocolVersion.MaxSupported);
            
            Assert.AreEqual("ut-cluster", metadata.ClusterName);
        }
    }
}