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
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("short")]
    public class IdempotenceAwareRetryPolicyTests : TestGlobals
    {

        /// <summary>
        /// Test IdempotenceAwareRetryPolicy. 
        /// 
        /// If write query write is idempotent it will retry according to child retry policy specified.
        /// 
        /// @jira_ticket CSHARP-461
        /// 
        /// @test_category connection:retry_policy
        /// </summary>
        [Test]
        public void ShouldUseChildRetryPolicy_OnWriteTimeout()
        {
            var tableName = TestUtils.GetUniqueTableName();
            var cql = $"INSERT INTO {tableName}(k, i) VALUES (0, 0)";
            using (var simulacronCluster = SimulacronCluster.CreateNew(1))
            using (var cluster = Cluster.Builder().AddContactPoint(simulacronCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                simulacronCluster.Prime(new
                {
                    when = new { query = cql },
                    then = new
                    {
                        result = "write_timeout",
                        consistency_level = 5,
                        received = 1,
                        block_for = 2,
                        delay_in_ms = 0,
                        message = "write_timeout",
                        ignore_on_prepare = false,
                        write_type = "SIMPLE"
                    }
                });

                var testPolicy = new TestRetryPolicy();
                var policy = new IdempotenceAwareRetryPolicy(testPolicy);

                Assert.Throws<WriteTimeoutException>(() => session.Execute(
                    new SimpleStatement(cql)
                        .SetIdempotence(true)
                        .SetConsistencyLevel(ConsistencyLevel.All)
                        .SetRetryPolicy(policy)));

                Assert.AreEqual(1L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));

                Interlocked.Exchange(ref testPolicy.WriteTimeoutCounter, 0);

                Assert.Throws<WriteTimeoutException>(() => session.Execute(
                    new SimpleStatement(cql)
                        .SetIdempotence(false)
                        .SetConsistencyLevel(ConsistencyLevel.All)
                        .SetRetryPolicy(policy)));

                Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));
            }
        }

        private class TestRetryPolicy : IRetryPolicy
        {
            public long ReadTimeoutCounter;
            public long WriteTimeoutCounter;
            public long UnavailableCounter;

            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
            {
                Interlocked.Increment(ref ReadTimeoutCounter);
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                Interlocked.Increment(ref WriteTimeoutCounter);
                return RetryDecision.Rethrow();
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                Interlocked.Increment(ref UnavailableCounter);
                return RetryDecision.Rethrow();
            }
        }
    }
}
