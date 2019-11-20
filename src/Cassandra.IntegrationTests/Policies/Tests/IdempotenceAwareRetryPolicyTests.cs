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
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category("short"), Category("realcluster")]
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
            const string keyspace = "idempotenceAwarepolicytestks";
            var options = new TestClusterOptions { CassandraYaml = new[] { "phi_convict_threshold: 16" } };
            var testCluster = TestClusterManager.CreateNew(2, options);

            using (var cluster = Cluster.Builder().AddContactPoint(testCluster.InitialContactPoint)
                                        .WithQueryTimeout(60000)
                                        .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                        .Build())
            {
                var session = cluster.Connect();
                var tableName = TestUtils.GetUniqueTableName();
                session.DeleteKeyspaceIfExists(keyspace);
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, keyspace, 2), ConsistencyLevel.All);
                session.ChangeKeyspace(keyspace);
                session.Execute(new SimpleStatement(string.Format("CREATE TABLE {0} (k int PRIMARY KEY, i int)", tableName)).SetConsistencyLevel(ConsistencyLevel.All));

                testCluster.PauseNode(2);

                var testPolicy = new TestRetryPolicy();
                var policy = new IdempotenceAwareRetryPolicy(testPolicy);

                try
                {
                    session.Execute(new SimpleStatement(string.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", tableName))
                        .SetIdempotence(true)
                        .SetConsistencyLevel(ConsistencyLevel.All)
                        .SetRetryPolicy(policy));
                }
                catch (WriteTimeoutException)
                {
                    //throws a WriteTimeoutException, as its set as an idempotent query, it will call the childPolicy
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.ReadTimeoutCounter));
                    Assert.AreEqual(1L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.UnavailableCounter));
                }
                catch (UnavailableException)
                {
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.ReadTimeoutCounter));
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));
                    Assert.AreEqual(1L, Interlocked.Read(ref testPolicy.UnavailableCounter));
                }
                catch (Exception e)
                {
                    Trace.TraceWarning(e.Message);
                }

                Interlocked.Exchange(ref testPolicy.UnavailableCounter, 0);
                Interlocked.Exchange(ref testPolicy.WriteTimeoutCounter, 0);
                Interlocked.Exchange(ref testPolicy.ReadTimeoutCounter, 0);

                //testing with unidempotent query
                try
                {
                    session.Execute(new SimpleStatement(string.Format("INSERT INTO {0}(k, i) VALUES (0, 0)", tableName))
                        .SetIdempotence(false)
                        .SetConsistencyLevel(ConsistencyLevel.All)
                        .SetRetryPolicy(policy));
                }
                catch (WriteTimeoutException)
                {
                    //throws a WriteTimeoutException, as its set as NOT an idempotent query, it will not call the childPolicy
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.ReadTimeoutCounter));
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.UnavailableCounter));
                }
                catch (UnavailableException)
                {
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.ReadTimeoutCounter));
                    Assert.AreEqual(0L, Interlocked.Read(ref testPolicy.WriteTimeoutCounter));
                    Assert.AreEqual(1L, Interlocked.Read(ref testPolicy.UnavailableCounter));
                }
                catch (Exception e)
                {
                    Trace.TraceWarning(e.Message);
                }
            }

            testCluster.Remove();
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
