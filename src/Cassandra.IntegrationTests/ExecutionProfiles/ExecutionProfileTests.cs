//
//       Copyright (C) 2019 DataStax Inc.
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

using System.Linq;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.ExecutionProfiles
{
    [TestFixture]
    [Category("short")]
    public class ExecutionProfileTests
    {
        private SimulacronCluster _simulacron;

        [SetUp]
        public void SetUp()
        {
            _simulacron = SimulacronCluster.CreateNew(3);
        }

        [TearDown]
        public void TearDown()
        {
            _simulacron.Dispose();
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void Should_UseDerivedProfileConsistency_When_DerivedProfileIsProvided(bool async)
        {
            var cluster =
                Cluster.Builder()
                       .AddContactPoint(_simulacron.InitialContactPoint)
                       .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                       .WithExecutionProfiles(opts => opts
                          .WithProfile("write", profile => profile
                            .WithLoadBalancingPolicy(new RoundRobinPolicy())
                            .WithConsistencyLevel(ConsistencyLevel.All))
                          .WithDerivedProfile("read", "write", derivedProfile => derivedProfile
                            .WithConsistencyLevel(ConsistencyLevel.Two)))
                       .Build();
            var session = cluster.Connect();
            var primeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "TWO" } },
                then = new
                {
                    result = "unavailable",
                    consistency_level = (int)ConsistencyLevel.All,
                    required = 3,
                    alive = 2,
                    delay_in_ms = 0,
                    message = "unavailable",
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(primeQuery);

            var exception = async
                ? Assert.ThrowsAsync<UnavailableException>(() => session.ExecuteAsync(new SimpleStatement("SELECT * from test.test"), "read"))
                : Assert.Throws<UnavailableException>(() => session.Execute("SELECT * from test.test", "read"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_UseProfileConsistency_When_ProfileIsProvided(bool async)
        {
            var cluster =
                Cluster.Builder()
                       .AddContactPoint(_simulacron.InitialContactPoint)
                       .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                       .WithExecutionProfiles(opts => opts
                           .WithProfile("write", profile => profile
                                .WithLoadBalancingPolicy(new RoundRobinPolicy())
                                .WithConsistencyLevel(ConsistencyLevel.All))
                           .WithDerivedProfile("read", "write", derivedProfile => derivedProfile
                                .WithConsistencyLevel(ConsistencyLevel.Two)))
                       .Build();
            var session = cluster.Connect();
            var primeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "TWO" } },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[] { "test6", "test5" }.Select(v => new { text = v }).ToArray(),
                    column_types = new { text = "ascii" },
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(primeQuery);

            var rs = async
                ? await session.ExecuteAsync(new SimpleStatement("SELECT * from test.test"), "read").ConfigureAwait(false)
                : session.Execute("SELECT * from test.test", "read");
            var rows = rs.ToList();
            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual("test6", rows[0].GetValue<string>("text"));
            Assert.AreEqual("test5", rows[1].GetValue<string>("text"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_UseClusterConsistency_When_ProfileIsNotProvided(bool async)
        {
            var cluster =
                Cluster.Builder()
                       .AddContactPoint(_simulacron.InitialContactPoint)
                       .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                       .WithExecutionProfiles(opts => opts
                          .WithProfile("write", profile => profile
                                .WithLoadBalancingPolicy(new RoundRobinPolicy())
                                .WithConsistencyLevel(ConsistencyLevel.All))
                          .WithDerivedProfile("read", "write", derivedProfile => derivedProfile
                                .WithConsistencyLevel(ConsistencyLevel.Two)))
                       .Build();
            var session = cluster.Connect();

            var primeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "ONE" } },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[] { "test10", "test60" }.Select(v => new { text = v }).ToArray(),
                    column_types = new { text = "ascii" },
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(primeQuery);

            var secondPrimeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "TWO" } },
                then = new
                {
                    result = "unavailable",
                    consistency_level = (int)ConsistencyLevel.Two,
                    required = 2,
                    alive = 1,
                    delay_in_ms = 0,
                    message = "unavailable",
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(secondPrimeQuery);

            var rs = async
                ? await session.ExecuteAsync(new SimpleStatement("SELECT * from test.test")).ConfigureAwait(false)
                : session.Execute("SELECT * from test.test");
            var rows = rs.ToList();
            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual("test10", rows[0].GetValue<string>("text"));
            Assert.AreEqual("test60", rows[1].GetValue<string>("text"));
            var exception = async
                ? Assert.ThrowsAsync<UnavailableException>(() => session.ExecuteAsync(new SimpleStatement("SELECT * from test.test"), "read"))
                : Assert.Throws<UnavailableException>(() => session.Execute("SELECT * from test.test", "read"));
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_UseDefaultProfileConsistency_When_ProfileIsNotProvidedButDefaultProfileWasChanged(bool async)
        {
            var cluster =
                Cluster.Builder()
                       .AddContactPoint(_simulacron.InitialContactPoint)
                       .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                       .WithExecutionProfiles(opts => opts
                          .WithProfile("write", profile => profile
                                .WithLoadBalancingPolicy(new RoundRobinPolicy())
                                .WithConsistencyLevel(ConsistencyLevel.All))
                          .WithDerivedProfile("read", "write", derivedProfile => derivedProfile
                                .WithConsistencyLevel(ConsistencyLevel.Two))
                          .WithProfile("default", defaultProfile => defaultProfile
                                .WithConsistencyLevel(ConsistencyLevel.Quorum)))
                       .Build();
            var session = cluster.Connect();

            var primeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "ONE" } },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[] { "test10", "test60" }.Select(v => new { text = v }).ToArray(),
                    column_types = new { text = "ascii" },
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(primeQuery);

            var secondPrimeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "TWO" } },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[] { "test12", "test62" }.Select(v => new { text = v }).ToArray(),
                    column_types = new { text = "ascii" },
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(secondPrimeQuery);
            
            var thirdPrimeQuery = new
            {
                when = new { query = "SELECT * from test.test", consistency_level = new[] { "QUORUM" } },
                then = new
                {
                    result = "unavailable",
                    consistency_level = (int)ConsistencyLevel.Two,
                    required = 2,
                    alive = 1,
                    delay_in_ms = 0,
                    message = "unavailable",
                    ignore_on_prepare = false
                }
            };
            _simulacron.Prime(thirdPrimeQuery);

            var rs = async
                ? await session.ExecuteAsync(new SimpleStatement("SELECT * from test.test"), "read").ConfigureAwait(false)
                : session.Execute("SELECT * from test.test", "read");
            var rows = rs.ToList();
            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual("test12", rows[0].GetValue<string>("text"));
            Assert.AreEqual("test62", rows[1].GetValue<string>("text"));
            var exception = async
                ? Assert.ThrowsAsync<UnavailableException>(() => session.ExecuteAsync(new SimpleStatement("SELECT * from test.test")))
                : Assert.Throws<UnavailableException>(() => session.Execute("SELECT * from test.test"));
        }
    }
}