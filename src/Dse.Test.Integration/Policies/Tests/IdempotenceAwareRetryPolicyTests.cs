//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Test.Integration.TestClusterManagement.Simulacron;
using NUnit.Framework;

namespace Dse.Test.Integration.Policies.Tests
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
                simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenWriteTimeout("write_timeout", 5, 1, 2, "SIMPLE"));

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
