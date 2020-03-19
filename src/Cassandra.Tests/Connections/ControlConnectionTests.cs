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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ProtocolEvents;
using Cassandra.SessionManagement;
using Cassandra.Tests.MetadataHelpers.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class ControlConnectionTests
    {
        private FakeConnectionFactory _connectionFactory;
        private IPEndPoint _endpoint1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
        private IPEndPoint _endpoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);
        private TestContactPoint _cp1;
        private TestContactPoint _cp2;
        private TestContactPoint _localhost;

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_ResolveContactPointsAndAttemptEveryOne_When_ContactPointResolutionReturnsMultiple(bool keepContactPointsUnresolved)
        {
            var target = Create(keepContactPointsUnresolved);

            Assert.ThrowsAsync<NoHostAvailableException>(() => target.InitAsync());

            if (keepContactPointsUnresolved)
            {
                Assert.AreEqual(0, _cp1.Calls.Count(b => !b));
                Assert.AreEqual(0, _cp2.Calls.Count(b => !b));
                Assert.AreEqual(0, _localhost.Calls.Count(b => !b));
            }
            else
            {
                Assert.AreEqual(1, _cp1.Calls.Count(b => !b));
                Assert.AreEqual(1, _cp2.Calls.Count(b => !b));
                Assert.AreEqual(1, _localhost.Calls.Count(b => !b));
            }

            Assert.AreEqual(1, _cp1.Calls.Count(b => b));
            Assert.AreEqual(1, _cp2.Calls.Count(b => b));
            Assert.AreEqual(1, _localhost.Calls.Count(b => b));
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint1].Count);
            Assert.AreEqual(2, _connectionFactory.CreatedConnections[_endpoint2].Count);
        }

        private IControlConnection Create(bool keepContactPointsUnresolved)
        {
            _connectionFactory = new FakeConnectionFactory();
            var config = new TestConfigurationBuilder
            {
                ConnectionFactory = _connectionFactory,
                KeepContactPointsUnresolved = keepContactPointsUnresolved
            }.Build();
            _cp1 = new TestContactPoint(new List<IConnectionEndPoint>
            {
                new ConnectionEndPoint(_endpoint1, config.ServerNameResolver, _cp1)
            });
            _cp2 = new TestContactPoint(new List<IConnectionEndPoint>
            {
                new ConnectionEndPoint(_endpoint2, config.ServerNameResolver, _cp2)
            });
            _localhost = new TestContactPoint(new List<IConnectionEndPoint>
            {
                new ConnectionEndPoint(_endpoint1, config.ServerNameResolver, _localhost),
                new ConnectionEndPoint(_endpoint2, config.ServerNameResolver, _localhost)
            });
            return new ControlConnection(
                Mock.Of<IInternalCluster>(),
                new ProtocolEventDebouncer(
                    new FakeTimerFactory(), TimeSpan.Zero, TimeSpan.Zero), 
                ProtocolVersion.V3, 
                config, 
                new Metadata(config), 
                new List<IContactPoint>
                {
                    _cp1,
                    _cp2,
                    _localhost
                });
        }

        private class TestContactPoint : IContactPoint
        {
            public ConcurrentQueue<bool> Calls { get; } = new ConcurrentQueue<bool>();

            private readonly IEnumerable<IConnectionEndPoint> _endPoints;

            public TestContactPoint(IEnumerable<IConnectionEndPoint> endPoints)
            {
                _endPoints = endPoints;
            }

            public bool Equals(IContactPoint other)
            {
                return Equals((object) other);
            }

            public override bool Equals(object obj)
            {
                return object.ReferenceEquals(this, obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((_endPoints != null ? _endPoints.GetHashCode() : 0) * 397) ^ (Calls != null ? Calls.GetHashCode() : 0);
                }
            }

            public bool CanBeResolved => true;

            public string StringRepresentation => "123";

            public Task<IEnumerable<IConnectionEndPoint>> GetConnectionEndPointsAsync(bool refreshCache)
            {
                Calls.Enqueue(refreshCache);
                return Task.FromResult(_endPoints);
            }
        }
    }
}