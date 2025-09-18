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
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using Newtonsoft.Json;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category(TestCategory.Short)]
    public class RetryPolicyShortTests : TestGlobals
    {
        public static object[] RetryPolicyExtendedData =
        {
            new object[] { ServerError.IsBootstrapping, typeof(IsBootstrappingException) },
            new object[] { ServerError.Overloaded, typeof(OverloadedException) }
        };

        [TestCaseSource(nameof(RetryPolicyShortTests.RetryPolicyExtendedData))]
        public void RetryPolicy_Extended(ServerError resultError, Type exceptionType)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions()))
            {
                var contactPoint = simulacronCluster.InitialContactPoint;
                var extendedRetryPolicy = new TestExtendedRetryPolicy();
                var builder = ClusterBuilder()
                                     .AddContactPoint(contactPoint)
                                     .WithRetryPolicy(extendedRetryPolicy)
                                     .WithReconnectionPolicy(new ConstantReconnectionPolicy(long.MaxValue));
                using (var cluster = builder.Build())
                {
                    var session = (Session)cluster.Connect();
                    const string cql = "select * from table1";

                    simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenServerError(resultError, resultError.Value));
                    Exception throwedException = null;
                    try
                    {
                        session.Execute(cql);
                    }
                    catch (Exception ex)
                    {
                        throwedException = ex;
                    }
                    finally
                    {
                        Assert.NotNull(throwedException);
                        Assert.AreEqual(throwedException.GetType(), exceptionType);
                        Assert.AreEqual(1, Interlocked.Read(ref extendedRetryPolicy.RequestErrorConter));
                        Assert.AreEqual(0, Interlocked.Read(ref extendedRetryPolicy.ReadTimeoutCounter));
                        Assert.AreEqual(0, Interlocked.Read(ref extendedRetryPolicy.WriteTimeoutCounter));
                        Assert.AreEqual(0, Interlocked.Read(ref extendedRetryPolicy.UnavailableCounter));
                    }
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public async Task Should_RetryOnNextHost_When_SendFailsOnCurrentHostRetryPolicy(bool async)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                var contactPoint = simulacronCluster.InitialContactPoint;
                var nodes = simulacronCluster.GetNodes().ToArray();
                var queryPlan = new List<SimulacronNode>
                {
                    nodes[1],
                    nodes[2],
                    nodes[0]
                };
                await queryPlan[0].Stop().ConfigureAwait(false);
                var currentHostRetryPolicy = new CurrentHostRetryPolicy(10, null);
                var loadBalancingPolicy = new CustomLoadBalancingPolicy(
                    queryPlan.Select(n => n.ContactPoint).ToArray());
                var builder = ClusterBuilder()
                                     .AddContactPoint(contactPoint)
                                     .WithSocketOptions(new SocketOptions()
                                                        .SetConnectTimeoutMillis(10000)
                                                        .SetReadTimeoutMillis(5000))
                                     .WithLoadBalancingPolicy(loadBalancingPolicy)
                                     .WithRetryPolicy(currentHostRetryPolicy);
                using (var cluster = builder.Build())
                {
                    var session = (Session)cluster.Connect();
                    const string cql = "select * from table2";

                    queryPlan[1].PrimeFluent(
                        b => b.WhenQuery(cql).
                               ThenRowsSuccess(new[] { ("text", DataType.Ascii) }, rows => rows.WithRow("test1").WithRow("test2")));

                    if (async)
                    {
                        await session.ExecuteAsync(
                            new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.One)).ConfigureAwait(false);
                    }
                    else
                    {
                        session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.One));
                    }

                    var queriesFirstNode = await queryPlan[0].GetQueriesAsync(cql).ConfigureAwait(false);
                    var queriesFirstNodeString = string.Join(Environment.NewLine, queriesFirstNode.Select<dynamic, string>(obj => JsonConvert.SerializeObject(obj)));
                    var queriesSecondNode = await queryPlan[1].GetQueriesAsync(cql).ConfigureAwait(false);
                    var queriesSecondNodeString = string.Join(Environment.NewLine, queriesSecondNode.Select<dynamic, string>(obj => JsonConvert.SerializeObject(obj)));
                    var queriesThirdNode = await queryPlan[2].GetQueriesAsync(cql).ConfigureAwait(false);
                    var queriesThirdNodeString = string.Join(Environment.NewLine, queriesThirdNode.Select<dynamic, string>(obj => JsonConvert.SerializeObject(obj)));
                    var allQueries = new { First = queriesFirstNodeString, Second = queriesSecondNodeString, Third = queriesThirdNodeString };
                    var allQueriesString = JsonConvert.SerializeObject(allQueries);

                    Assert.AreEqual(0, currentHostRetryPolicy.RequestErrorCounter, allQueriesString);
                    Assert.AreEqual(0, queriesFirstNode.Count, allQueriesString);
                    Assert.AreEqual(1, queriesSecondNode.Count, allQueriesString);
                    Assert.AreEqual(0, queriesThirdNode.Count, allQueriesString);
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public async Task Should_KeepRetryingOnSameHost_When_CurrentHostRetryPolicyIsSetAndSendSucceeds(bool async)
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                var contactPoint = simulacronCluster.InitialContactPoint;
                var nodes = simulacronCluster.GetNodes().ToArray();
                var currentHostRetryPolicy = new CurrentHostRetryPolicy(10, null);
                var loadBalancingPolicy = new CustomLoadBalancingPolicy(
                    nodes.Select(n => n.ContactPoint).ToArray());
                var builder = ClusterBuilder()
                                     .AddContactPoint(contactPoint)
                                     .WithSocketOptions(new SocketOptions()
                                                        .SetConnectTimeoutMillis(10000)
                                                        .SetReadTimeoutMillis(5000))
                                     .WithLoadBalancingPolicy(loadBalancingPolicy)
                                     .WithRetryPolicy(currentHostRetryPolicy);
                using (var cluster = builder.Build())
                {
                    var session = (Session)cluster.Connect();
                    const string cql = "select * from table2";

                    nodes[0].PrimeFluent(
                        b => b.WhenQuery(cql).
                               ThenOverloaded("overloaded"));

                    nodes[1].PrimeFluent(
                        b => b.WhenQuery(cql).
                               ThenRowsSuccess(new[] { ("text", DataType.Ascii) }, rows => rows.WithRow("test1").WithRow("test2")));

                    if (async)
                    {
                        Assert.ThrowsAsync<OverloadedException>(() => session.ExecuteAsync(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.One)));
                    }
                    else
                    {
                        Assert.Throws<OverloadedException>(() => session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.One)));
                    }

                    Assert.AreEqual(11, currentHostRetryPolicy.RequestErrorCounter);
                    Assert.AreEqual(12, (await nodes[0].GetQueriesAsync(cql).ConfigureAwait(false)).Count);
                    Assert.AreEqual(0, (await nodes[1].GetQueriesAsync(cql).ConfigureAwait(false)).Count);
                    Assert.AreEqual(0, (await nodes[2].GetQueriesAsync(cql).ConfigureAwait(false)).Count);
                }
            }
        }

        private class TestExtendedRetryPolicy : IExtendedRetryPolicy
        {
            public long ReadTimeoutCounter;
            public long WriteTimeoutCounter;
            public long UnavailableCounter;
            public long RequestErrorConter;

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                               int nbRetry)
            {
                Interlocked.Increment(ref ReadTimeoutCounter);
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks,
                                                int nbRetry)
            {
                Interlocked.Increment(ref WriteTimeoutCounter);
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                Interlocked.Increment(ref UnavailableCounter);
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                Interlocked.Increment(ref RequestErrorConter);
                return RetryDecision.Rethrow();
            }
        }

        private class CurrentHostRetryPolicy : IExtendedRetryPolicy
        {
            private readonly int _maxRetries;
            private readonly Func<int, Task> _action;
            public long RequestErrorCounter;

            public CurrentHostRetryPolicy(int maxRetries, Func<int, Task> action)
            {
                _maxRetries = maxRetries;
                _action = action;
            }

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnRequestError(IStatement statement, Configuration config, Exception ex, int nbRetry)
            {
                _action?.Invoke(nbRetry).GetAwaiter().GetResult();

                if (nbRetry > _maxRetries)
                {
                    return RetryDecision.Rethrow();
                }

                Interlocked.Increment(ref RequestErrorCounter);
                return RetryDecision.Retry(null, true);
            }
        }
    }
}