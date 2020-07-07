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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.ProtocolEvents;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

using Moq;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeControlConnectionFactory : IControlConnectionFactory
    {
        public IControlConnection Create(
            IInternalCluster cluster,
            Configuration config,
            IInternalMetadata metadata,
            IEnumerable<IContactPoint> contactPoints)
        {
            var cc = Mock.Of<IControlConnection>();
            Mock.Get(cc).Setup(c => c.InitAsync(It.IsAny<CancellationToken>())).Returns(Task.Run(async () =>
            {
                var cps = new Dictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>();
                foreach (var cp in contactPoints)
                {
                    var connectionEndpoints = (await cp.GetConnectionEndPointsAsync(true).ConfigureAwait(false)).ToList();
                    cps.Add(cp, connectionEndpoints);
                    foreach (var connectionEndpoint in connectionEndpoints)
                    {
                        var endpt = connectionEndpoint.GetHostIpEndPointWithFallback();
                        var host = metadata.AddHost(endpt, cp);
                        host.SetInfo(BuildRow());
                        Mock.Get(cc).Setup(c => c.Host).Returns(host);
                    }
                }
                metadata.SetResolvedContactPoints(cps);
                config.SerializerManager.ChangeProtocolVersion(ProtocolVersion.V3);
            }));
            return cc;
        }

        private IRow BuildRow(Guid? hostId = null)
        {
            return new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "host_id", hostId ?? Guid.NewGuid() },
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "release_version", "3.11.1" },
                { "tokens", new List<string> { "1" }}
            });
        }
    }
}