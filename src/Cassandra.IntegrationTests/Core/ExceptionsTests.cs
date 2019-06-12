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

using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short")]
    public class ExceptionsTests : TestGlobals
    {
        private ISession _session;
        private string _keyspace;
        private string _table;
        private static SimulacronCluster _simulacronCluster;

        [SetUp]
        public void RestartCluster()
        {
            _simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions());
            var contactPoint = _simulacronCluster.InitialContactPoint;
            var builder = Cluster.Builder()
                                 .AddContactPoint(contactPoint);
            var cluster = builder.Build();
            _session = cluster.Connect();
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _table = TestUtils.GetUniqueTableName().ToLowerInvariant();
        }

        [TearDown]
        public void TearDown()
        {
            _session.Cluster.Shutdown();
            _simulacronCluster.Remove();
        }

        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [Test]
        public void AlreadyExistsException()
        {
            var cql = string.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, 1);

            var primeQuery = new
            {
                when = new { query = cql },
                then = new
                {
                    result = "already_exists",
                    delay_in_ms = 0,
                    message = "already_exists",
                    keyspace = _keyspace,
                    table = "",
                    ignore_on_prepare = false
                }
            };

            _simulacronCluster.Prime(primeQuery);

            var ex = Assert.Throws<AlreadyExistsException>(() => _session.Execute(cql));
            Assert.AreEqual(ex.Keyspace, _keyspace);
            Assert.AreEqual(ex.Table, null);
            Assert.AreEqual(ex.WasTableCreation, false);


            var cqlTable = string.Format(TestUtils.CreateTableSimpleFormat, _table);

            var primeQueryTable = new
            {
                when = new { query = cqlTable },
                then = new
                {
                    result = "already_exists",
                    delay_in_ms = 0,
                    message = "already_exists",
                    table = _table,
                    keyspace = _keyspace,
                    ignore_on_prepare = false
                }
            };

            _simulacronCluster.Prime(primeQueryTable);
            var e = Assert.Throws<AlreadyExistsException>(() => _session.Execute(cqlTable));
            Assert.AreEqual(e.Keyspace, _keyspace);
            Assert.AreEqual(e.Table, _table);
            Assert.AreEqual(e.WasTableCreation, true);
        }

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [Test]
        public void NoHostAvailableException()
        {
            const string ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception> {{IPAddress.Parse(ipAddress), null}};

            try
            {
                Cluster.Builder().AddContactPoint(ipAddress).Build();
            }
            catch (NoHostAvailableException e)
            {
                Assert.AreEqual(e.Message, $"All host tried for query are in error (tried: {ipAddress})");
                Assert.AreEqual(e.Errors.Keys.ToArray(), errorsHashMap.Keys.ToArray());
            }
        }

        /// <summary>
        ///  Tests the ReadTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt a read of
        ///  the key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void ReadTimeoutException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "read_timeout",
                    consistency_level = 5,
                    received = 1,
                    block_for = 2,
                    data_present = true,
                    delay_in_ms = 0,
                    message = "read_timeout",
                    ignore_on_prepare = false
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<ReadTimeoutException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
            Assert.AreEqual(ex.ReceivedAcknowledgements, 1);
            Assert.AreEqual(ex.RequiredAcknowledgements, 2);
            Assert.AreEqual(ex.WasDataRetrieved, true);
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void SyntaxError()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new SyntaxError(errorMessage);
            }
            catch (SyntaxError e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TraceRetrievalException. Tests basic message.
        /// </summary>
        [Test]
        public void TraceRetrievalException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TraceRetrievalException(errorMessage);
            }
            catch (TraceRetrievalException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests TruncateException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void TruncateException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TruncateException(errorMessage);
            }
            catch (TruncateException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests UnauthorizedException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void UnauthorizedException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new UnauthorizedException(errorMessage);
            }
            catch (UnauthorizedException e)
            {
                Assert.AreEqual(e.Message, errorMessage);
            }
        }

        /// <summary>
        ///  Tests the UnavailableException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then kill single node, wait for gossip to propogate the
        ///  new state, and attempt to read and write the same key at CL.ALL. Catch and
        ///  test all available exception methods.
        /// </summary>
        [Test]
        public void UnavailableException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "unavailable",
                    consistency_level = 5,
                    required = 2,
                    alive = 1,
                    delay_in_ms = 0,
                    message = "unavailable",
                    ignore_on_prepare = false
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<UnavailableException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.Consistency, ConsistencyLevel.All);
            Assert.AreEqual(ex.RequiredReplicas, 2);
            Assert.AreEqual(ex.AliveReplicas, 1);
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void WriteTimeoutException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
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
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<WriteTimeoutException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
            Assert.AreEqual(1, ex.ReceivedAcknowledgements);
            Assert.AreEqual(2, ex.RequiredAcknowledgements);
            Assert.AreEqual(ex.WriteType, "SIMPLE");
        }

        [Test]
        public void PreserveStackTraceTest()
        {
            var primeQuery = new
            {
                when = new
                {
                    query = "SELECT WILL FAIL"
                },
                then = new
                {
                    result = "syntax_error",
                    delay_in_ms = 0,
                    message = "syntax_error",
                    ignore_on_prepare = false,
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<SyntaxError>(() => _session.Execute("SELECT WILL FAIL"));
#if !NETCORE || DEBUG
            // On .NET Core using Release compilation, the stack trace is limited
            StringAssert.Contains(nameof(PreserveStackTraceTest), ex.StackTrace);
            StringAssert.Contains(nameof(ExceptionsTests), ex.StackTrace);
#endif
        }

        [Test]
        public void RowSetIteratedTwice()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[]
                    {
                        new
                        {
                            id = Guid.NewGuid(),
                            value = "value"
                        }
                    },
                    column_types = new
                    {
                        id = "uuid",
                        value = "varchar"
                    }
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var rowset = _session.Execute(new SimpleStatement(cql)).GetRows();
            Assert.NotNull(rowset);
            //Linq Count iterates
            Assert.AreEqual(1, rowset.Count());
            Assert.AreEqual(0, rowset.Count());
        }

        [Test]
        public void RowSetPagingAfterSessionDispose()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "success",
                    delay_in_ms = 0,
                    rows = new[]
                    {
                        new
                        {
                            id = Guid.NewGuid(),
                            value = "value"
                        }
                    },
                    column_types = new
                    {
                        id = "uuid",
                        value = "varchar"
                    }
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var rs = _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetPageSize(1));
            if (CassandraVersion.Major < 2)
            {
                //Paging should be ignored in 1.x
                //But setting the page size should work
                Assert.AreEqual(2, rs.InnerQueueCount);
                return;
            }
            Assert.AreEqual(1, rs.InnerQueueCount);

            _session.Dispose();
            //It should not fail, just do nothing
            rs.FetchMoreResults();
            Assert.AreEqual(1, rs.InnerQueueCount);
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void WriteFailureExceptionTest()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "write_failure",
                    consistency_level = 5,
                    received = 1,
                    block_for = 2,
                    delay_in_ms = 0,
                    message = "write_failure",
                    ignore_on_prepare = false,
                    failure_reasons = new Dictionary<string, int> { { "127.0.0.1", 0 } },
                    write_type = "SIMPLE"
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<WriteFailureException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.AreEqual(ex.ConsistencyLevel, ConsistencyLevel.All);
            Assert.AreEqual(1, ex.ReceivedAcknowledgements);
            Assert.AreEqual(2, ex.RequiredAcknowledgements);
            Assert.AreEqual(ex.WriteType, "SIMPLE");
        }

        [Test]
        [TestCase(ConsistencyLevel.LocalQuorum, 2, 5, true,
                  "LocalQuorum (5 response(s) were required but only 2 replica(s) responded, 1 failed)")]
        [TestCase(ConsistencyLevel.LocalQuorum, 1, 2, true,
                  "LocalQuorum (2 response(s) were required but only 1 replica(s) responded, 1 failed)")]
        [TestCase(ConsistencyLevel.LocalOne, 1, 0, false,
                  "LocalOne (the replica queried for data didn't respond)")]
        [TestCase(ConsistencyLevel.LocalQuorum, 3, 3, true,
                  "LocalQuorum (failure while waiting for repair of inconsistent replica)")]
        public void ReadFailureExceptionTest(ConsistencyLevel consistencyLevel, int received, int required,
                                             bool dataPresent, string expectedMessageEnd)
        {

            const string baseMessage = "Server failure during read query at consistency ";
            const string cql = "SELECT * FROM ks1.table_for_read_failure_test";
            var primeQuery = new
            {
                when = new
                {
                    query = cql
                },
                then = new
                {
                    result = "read_failure",
                    consistency_level = (int) consistencyLevel,
                    received,
                    block_for = required,
                    delay_in_ms = 0,
                    data_present = dataPresent,
                    message = "read_failure",
                    ignore_on_prepare = false,
                    failure_reasons = new Dictionary<string, int> { { "127.0.0.1", 0 } }
                }
            };

            _simulacronCluster.Prime(primeQuery);
            var ex = Assert.Throws<ReadFailureException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(consistencyLevel)));
            Assert.AreEqual(consistencyLevel, ex.ConsistencyLevel);
            Assert.AreEqual(received, ex.ReceivedAcknowledgements);
            Assert.AreEqual(required, ex.RequiredAcknowledgements);
            Assert.AreEqual(baseMessage + expectedMessageEnd, ex.Message);
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void FunctionFailureExceptionTest()
        {
            const string cql = "SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1";
            var primeQuery = new
            {
                when = new { query = cql },
                then = new
                {
                    result = "function_failure",
                    keyspace = "ks_func",
                    function = "div",
                    arg_types = new [] {"text"},
                    detail = "function_failure",
                    delay_in_ms = 0,
                    ignore_on_prepare = false
                }
            };

            _simulacronCluster.Prime(primeQuery);

            Assert.Throws<FunctionFailureException>(() => _session.Execute(cql));
        }
    }
}
