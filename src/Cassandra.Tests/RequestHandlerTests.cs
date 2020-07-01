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
using System.IO;
using System.Linq;
using System.Text;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;

using NUnit.Framework;

using PrepareFlags = Cassandra.Requests.PrepareRequest.PrepareFlags;
using QueryFlags = Cassandra.QueryProtocolOptions.QueryFlags;

namespace Cassandra.Tests
{
    [TestFixture]
    public class RequestHandlerTests
    {
        private static readonly ISerializerManager SerializerManager = new SerializerManager(ProtocolVersion.MaxSupported);
        private static readonly ISerializer Serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();

        private static Configuration GetConfig(QueryOptions queryOptions = null, Cassandra.Policies policies = null, PoolingOptions poolingOptions = null)
        {
            return new TestConfigurationBuilder
            {
                Policies = policies ?? new Cassandra.Policies(),
                PoolingOptions = poolingOptions,
                QueryOptions = queryOptions ?? DefaultQueryOptions
            }.Build();
        }

        private static IRequestOptions GetRequestOptions(QueryOptions queryOptions = null, Cassandra.Policies policies = null)
        {
            return RequestHandlerTests.GetConfig(queryOptions, policies).DefaultRequestOptions;
        }

        private static QueryOptions DefaultQueryOptions => new QueryOptions();

        private static PreparedStatement GetPrepared(
            byte[] queryId = null, ISerializerManager serializerManager = null, RowSetMetadata rowSetMetadata = null)
        {
            return new PreparedStatement(
                null, 
                queryId, 
                new ResultMetadata(new byte[16], rowSetMetadata), 
                "DUMMY QUERY", 
                null, 
                serializerManager ?? RequestHandlerTests.SerializerManager);
        }

        [Test]
        public void RequestHandler_GetRequest_SimpleStatement_Default_QueryOptions_Are_Used()
        {
            var stmt = new SimpleStatement("DUMMY QUERY");
            Assert.AreEqual(0, stmt.PageSize);
            Assert.Null(stmt.ConsistencyLevel);
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(DefaultQueryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(DefaultQueryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_SimpleStatement_QueryOptions_Are_Used()
        {
            var stmt = new SimpleStatement("DUMMY QUERY");
            Assert.AreEqual(0, stmt.PageSize);
            Assert.Null(stmt.ConsistencyLevel);
            var queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum).SetPageSize(100);
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions(queryOptions));
            Assert.AreEqual(100, request.PageSize);
            Assert.AreEqual(queryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
            Assert.AreEqual(queryOptions.GetSerialConsistencyLevel(), request.SerialConsistency);
        }

        [Test]
        public void RequestHandler_GetRequest_SimpleStatement_Statement_Level_Settings_Are_Used()
        {
            var stmt = new SimpleStatement("DUMMY QUERY");
            stmt.SetConsistencyLevel(ConsistencyLevel.EachQuorum);
            stmt.SetPageSize(350);
            stmt.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            Assert.AreEqual(350, stmt.PageSize);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, stmt.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, stmt.SerialConsistencyLevel);
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(350, request.PageSize);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, request.Consistency);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, request.SerialConsistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BoundStatement_Default_QueryOptions_Are_Used()
        {
            var ps = GetPrepared();
            var stmt = ps.Bind();
            Assert.AreEqual(0, stmt.PageSize);
            Assert.Null(stmt.ConsistencyLevel);
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(DefaultQueryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(DefaultQueryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BoundStatement_QueryOptions_Are_Used()
        {
            var ps = GetPrepared();
            var stmt = ps.Bind();
            Assert.AreEqual(0, stmt.PageSize);
            Assert.Null(stmt.ConsistencyLevel);
            var queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum).SetPageSize(100);
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions(queryOptions));
            Assert.AreEqual(100, request.PageSize);
            Assert.AreEqual(queryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
            Assert.AreEqual(QueryOptions.DefaultSerialConsistencyLevel, request.SerialConsistency);
        }

        [Test]
        [TestCase(ProtocolVersion.V5)]
        [TestCase(ProtocolVersion.DseV2)]
        [TestCase(ProtocolVersion.V4)]
        [TestCase(ProtocolVersion.V3)]
        [TestCase(ProtocolVersion.V2)]
        [TestCase(ProtocolVersion.V1)]
        public void Should_NotSkipMetadata_When_BoundStatementDoesNotContainColumnDefinitions(ProtocolVersion version)
        {
            var serializerManager = new SerializerManager(version);
            var ps = GetPrepared(serializerManager: serializerManager);
            var stmt = ps.Bind();
            var queryOptions = new QueryOptions();
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, serializerManager.GetCurrentSerializer(), GetRequestOptions(queryOptions));
            Assert.IsFalse(request.SkipMetadata);
        }

        [Test]
        [TestCase(ProtocolVersion.V5, true)]
        [TestCase(ProtocolVersion.DseV2, true)]
        [TestCase(ProtocolVersion.V4, false)]
        [TestCase(ProtocolVersion.V3, false)]
        [TestCase(ProtocolVersion.V2, false)]
        [TestCase(ProtocolVersion.V1, false)]
        public void Should_SkipMetadata_When_BoundStatementContainsColumnDefinitionsAndProtocolSupportsNewResultMetadataId(
            ProtocolVersion version, bool isSet)
        {
            var serializerManager = new SerializerManager(version);
            var rsMetadata = new RowSetMetadata { Columns = new[] { new CqlColumn() } };
            var ps = GetPrepared(new byte[16], serializerManager, rsMetadata);
            var stmt = ps.Bind();
            var queryOptions = new QueryOptions();
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, serializerManager.GetCurrentSerializer(), GetRequestOptions(queryOptions));
            Assert.AreEqual(isSet, request.SkipMetadata);
        }
        
        [Test]
        [TestCase(ProtocolVersion.V5)]
        [TestCase(ProtocolVersion.DseV2)]
        [TestCase(ProtocolVersion.V4)]
        [TestCase(ProtocolVersion.V3)]
        [TestCase(ProtocolVersion.V2)]
        [TestCase(ProtocolVersion.V1)]
        public void Should_SkipMetadata_When_NotBoundStatement(ProtocolVersion version)
        {
            var serializerManager = new SerializerManager(version);
            var stmt = new SimpleStatement("DUMMY QUERY");
            var queryOptions = new QueryOptions();
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, serializerManager.GetCurrentSerializer(), GetRequestOptions(queryOptions));
            Assert.IsFalse(request.SkipMetadata);
        }

        [Test]
        public void RequestHandler_GetRequest_BoundStatement_Statement_Level_Settings_Are_Used()
        {
            var ps = GetPrepared();
            var stmt = ps.Bind();
            stmt.SetConsistencyLevel(ConsistencyLevel.EachQuorum);
            stmt.SetPageSize(350);
            stmt.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            Assert.AreEqual(350, stmt.PageSize);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, stmt.ConsistencyLevel);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, stmt.SerialConsistencyLevel);
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(350, request.PageSize);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, request.Consistency);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, request.SerialConsistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_Default_QueryOptions_Are_Used()
        {
            var stmt = new BatchStatement();
            Assert.Null(stmt.ConsistencyLevel);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(DefaultQueryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_QueryOptions_Are_Used()
        {
            var stmt = new BatchStatement();
            Assert.Null(stmt.ConsistencyLevel);
            var queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions(queryOptions));
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_Statement_Level_Settings_Are_Used()
        {
            var stmt = new BatchStatement();
            stmt.SetConsistencyLevel(ConsistencyLevel.EachQuorum);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, stmt.ConsistencyLevel);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetRequestOptions());
            Assert.AreEqual(ConsistencyLevel.EachQuorum, request.Consistency);
        }

        [Test]
        public void RequestExecution_GetRetryDecision_Test()
        {
            var config = new Configuration();
            var policy = Cassandra.Policies.DefaultRetryPolicy as IExtendedRetryPolicy;
            var statement = new SimpleStatement("SELECT WILL FAIL");
            //Using default retry policy the decision will always be to rethrow on read/write timeout
            var expected = RetryDecision.RetryDecisionType.Rethrow;
            var decision = GetRetryDecisionFromServerError(
                new ReadTimeoutException(ConsistencyLevel.Quorum, 1, 2, true), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = GetRetryDecisionFromServerError(
                new WriteTimeoutException(ConsistencyLevel.Quorum, 1, 2, "SIMPLE"), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = GetRetryDecisionFromServerError(
                new UnavailableException(ConsistencyLevel.Quorum, 2, 1), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = GetRetryDecisionFromClientError(
                new Exception(), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            //Expecting to retry when a Cassandra node is Bootstrapping/overloaded
            expected = RetryDecision.RetryDecisionType.Retry;
            decision = GetRetryDecisionFromServerError(
                new OverloadedException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = GetRetryDecisionFromServerError(
                new IsBootstrappingException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = GetRetryDecisionFromServerError(
                new TruncateException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
        }

        [Test]
        public void GetRequest_With_Timestamp_Generator()
        {
            // Timestamp generator should be enabled by default
            var statement = new SimpleStatement("QUERY");
            var config = new Configuration();

            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <query><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.False(flags.HasFlag(QueryFlags.Values));
            Assert.False(flags.HasFlag(QueryFlags.WithPagingState));
            Assert.False(flags.HasFlag(QueryFlags.SkipMetadata));
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
            // Skip result_page_size (4) + serial_consistency (2)
            offset += 6;
            var timestamp = BeConverter.ToInt64(bodyBuffer, offset);
            var expectedTimestamp = TypeSerializer.SinceUnixEpoch(DateTimeOffset.Now.Subtract(TimeSpan.FromMilliseconds(100))).Ticks / 10;
            Assert.Greater(timestamp, expectedTimestamp);
        }

        [Test]
        public void GetRequest_With_Timestamp_Generator_Empty_Value()
        {
            var statement = new SimpleStatement("QUERY");
            var policies = new Cassandra.Policies(
                Cassandra.Policies.DefaultLoadBalancingPolicy, Cassandra.Policies.DefaultReconnectionPolicy,
                Cassandra.Policies.DefaultRetryPolicy, Cassandra.Policies.DefaultSpeculativeExecutionPolicy,
                new NoTimestampGenerator());
            var config = RequestHandlerTests.GetConfig(new QueryOptions(), policies, PoolingOptions.Create());

            var request = RequestHandler.GetRequest(
                statement, RequestHandlerTests.Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <query><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                                         .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                                         .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.False(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.False(flags.HasFlag(QueryFlags.Values));
            Assert.False(flags.HasFlag(QueryFlags.WithPagingState));
            Assert.False(flags.HasFlag(QueryFlags.SkipMetadata));
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
        }

        [Test]
        public void GetRequest_With_Timestamp_Generator_Empty_Value_With_Statement_Timestamp()
        {
            var statement = new SimpleStatement("STATEMENT WITH TIMESTAMP");
            var expectedTimestamp = new DateTimeOffset(2010, 04, 29, 1, 2, 3, 4, TimeSpan.Zero).AddTicks(20);
            statement.SetTimestamp(expectedTimestamp);
            var policies = new Cassandra.Policies(
                Cassandra.Policies.DefaultLoadBalancingPolicy, Cassandra.Policies.DefaultReconnectionPolicy,
                Cassandra.Policies.DefaultRetryPolicy, Cassandra.Policies.DefaultSpeculativeExecutionPolicy,
                new NoTimestampGenerator());
            var config = RequestHandlerTests.GetConfig(new QueryOptions(), policies, PoolingOptions.Create());
            
            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <query><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                                         .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                                         .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            // Skip result_page_size (4) + serial_consistency (2)
            offset += 6;
            var timestamp = BeConverter.ToInt64(bodyBuffer, offset);
            Assert.AreEqual(TypeSerializer.SinceUnixEpoch(expectedTimestamp).Ticks / 10, timestamp);
        }

        [Test]
        public void GetRequest_Batch_With_64K_Queries()
        {
            var batch = new BatchStatement();
            for (var i = 0; i < ushort.MaxValue; i++)
            {
                batch.Add(new SimpleStatement("QUERY"));
            }

            var config = RequestHandlerTests.GetConfig(new QueryOptions(), Cassandra.Policies.DefaultPolicies, PoolingOptions.Create());
            var request = RequestHandler.GetRequest(batch, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            CollectionAssert.AreEqual(new byte[] { 0xff, 0xff }, bodyBuffer.Skip(1).Take(2));
        }

        [Test]
        public void GetRequest_Batch_With_Timestamp_Generator()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var startDate = DateTimeOffset.Now;
            // To microsecond precision
            startDate = startDate.Subtract(TimeSpan.FromTicks(startDate.Ticks % 10));

            var config = RequestHandlerTests.GetConfig(new QueryOptions(), Cassandra.Policies.DefaultPolicies, PoolingOptions.Create());
            var request = RequestHandler.GetRequest(batch, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var offset = 1;
            // n = 1
            Assert.AreEqual(1, BeConverter.ToInt16(bodyBuffer, offset));
            // Query_1 <kind><string><n_params>
            offset += 2;
            // kind = 0, not prepared
            Assert.AreEqual(0, bodyBuffer[offset++]);
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            // Skip serial consistency
            offset += 2;
            var timestamp = TypeSerializer.UnixStart.AddTicks(BeConverter.ToInt64(bodyBuffer, offset) * 10);
            Assert.GreaterOrEqual(timestamp, startDate);
            Assert.LessOrEqual(timestamp, DateTimeOffset.Now.Add(TimeSpan.FromMilliseconds(100)));
        }

        [Test]
        public void GetRequest_Batch_With_Empty_Timestamp_Generator()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var policies = new Cassandra.Policies(
                Cassandra.Policies.DefaultLoadBalancingPolicy, Cassandra.Policies.DefaultReconnectionPolicy,
                Cassandra.Policies.DefaultRetryPolicy, Cassandra.Policies.DefaultSpeculativeExecutionPolicy,
                new NoTimestampGenerator());

            var config = RequestHandlerTests.GetConfig(new QueryOptions(), policies, PoolingOptions.Create());
            var request = RequestHandler.GetRequest(batch, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.False(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            // Only serial consistency left
            Assert.AreEqual(bodyBuffer.Length, offset + 2);
        }

        [Test]
        public void GetRequest_Batch_With_Provided_Timestamp()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var providedTimestamp = DateTimeOffset.Now;
            // To microsecond precision
            providedTimestamp = providedTimestamp.Subtract(TimeSpan.FromTicks(providedTimestamp.Ticks % 10));
            batch.SetTimestamp(providedTimestamp);

            var config = RequestHandlerTests.GetConfig(new QueryOptions(), Cassandra.Policies.DefaultPolicies, PoolingOptions.Create());
            var request = RequestHandler.GetRequest(batch, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            // Skip serial consistency
            offset += 2;
            var timestamp = TypeSerializer.UnixStart.AddTicks(BeConverter.ToInt64(bodyBuffer, offset) * 10);
            Assert.AreEqual(providedTimestamp, timestamp);
        }

        [Test]
        public void GetRequest_Batch_With_Provided_Keyspace()
        {
            const string keyspace = "test_keyspace";
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY")).SetKeyspace(keyspace);

            var request = RequestHandler.GetRequest(batch, Serializer, new Configuration().DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>][<keyspace>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithKeyspace));

            // Skip serial consistency and timestamp
            offset +=
                (flags.HasFlag(QueryFlags.WithSerialConsistency) ? 2 : 0) +
                (flags.HasFlag(QueryFlags.WithDefaultTimestamp) ? 8 : 0);

            var keyspaceLength = BeConverter.ToInt16(bodyBuffer, offset);
            offset += 2;
            Assert.AreEqual(keyspace, Encoding.UTF8.GetString(bodyBuffer, offset, keyspaceLength));
        }

        [Test]
        public void GetRequest_Batch_With_Provided_Keyspace_On_Older_Protocol_Versions_Should_Ignore()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY")).SetKeyspace("test_keyspace");
            var serializer = new SerializerManager(ProtocolVersion.V3).GetCurrentSerializer();
            var request = RequestHandler.GetRequest(batch, serializer, new Configuration().DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request, serializer);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>][<keyspace>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.False(flags.HasFlag(QueryFlags.WithKeyspace));
        }

        [Test]
        public void GetRequest_Batch_With_SerialConsistency_On_Older_Protocol_Versions_Should_Ignore()
        {
            const string query = "QUERY";
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement(query))
                 .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            var serializer = new SerializerManager(ProtocolVersion.V2).GetCurrentSerializer();

            var request = RequestHandler.GetRequest(batch, serializer, new Configuration().DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request, serializer);

            // The batch request on protocol 2 is composed by:
            // <type><n><query_1>...<query_n><consistency>
            const int queryOffSet = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, queryOffSet);
            Assert.AreEqual(query.Length, queryLength);
            // query, n_params and consistency
            Assert.AreEqual(4 + 4 + queryLength + 2 + 2, bodyBuffer.Length);
        }

        [Test]
        public void GetRequest_Batch_Should_Use_Serial_Consistency_From_QueryOptions()
        {
            const string query = "QUERY";
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement(query));

            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.LocalSerial;
            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(expectedSerialConsistencyLevel);

            AssertBatchSerialConsistencyLevel(batch, config, query, expectedSerialConsistencyLevel);
        }

        [Test]
        public void GetRequest_Batch_Should_Use_Serial_Consistency_From_Statement()
        {
            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.LocalSerial;

            const string query = "QUERY";
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement(query))
                 .SetSerialConsistencyLevel(expectedSerialConsistencyLevel);

            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(ConsistencyLevel.Serial);

            AssertBatchSerialConsistencyLevel(batch, config, query, expectedSerialConsistencyLevel);
        }

        [Test]
        public void GetRequest_Execute_Should_Use_Serial_Consistency_From_Statement()
        {
            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.Serial;
            var ps = GetPrepared(queryId: new byte[16]);
            var statement = ps.Bind().SetSerialConsistencyLevel(expectedSerialConsistencyLevel);
            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);

            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The execute request is composed by:
            // <query_id><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            // Skip the queryid and consistency (2)
            var offset = 2 + ps.Id.Length + 2;
            if (Serializer.ProtocolVersion.SupportsResultMetadataId())
            {
                // Short bytes: 2 + 16
                offset += 18;
            }
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
            // Skip result_page_size (4)
            offset += 4;
            Assert.That((ConsistencyLevel)BeConverter.ToInt16(bodyBuffer, offset), Is.EqualTo(expectedSerialConsistencyLevel));
        }

        [Test]
        public void GetRequest_Execute_Should_Use_Serial_Consistency_From_QueryOptions()
        {
            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.LocalSerial;
            var ps = GetPrepared(queryId: new byte[16]);
            var statement = ps.Bind();
            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(expectedSerialConsistencyLevel);

            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The execute request is composed by:
            // <query_id><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            // Skip the queryid and consistency (2)
            var offset = 2 + ps.Id.Length + 2;
            if (Serializer.ProtocolVersion.SupportsResultMetadataId())
            {
                // Short bytes: 2 + 16
                offset += 18;
            }
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
            // Skip result_page_size (4)
            offset += 4;
            Assert.That((ConsistencyLevel)BeConverter.ToInt16(bodyBuffer, offset), Is.EqualTo(expectedSerialConsistencyLevel));
        }

        [Test]
        public void GetRequest_Query_Should_Use_Serial_Consistency_From_Statement()
        {
            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.LocalSerial;
            var statement = new SimpleStatement("QUERY");
            statement.SetSerialConsistencyLevel(expectedSerialConsistencyLevel);

            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(ConsistencyLevel.Serial);

            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <query><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            // Skip the query and consistency (2)
            var offset = 4 + statement.QueryString.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            // Skip result_page_size (4)
            offset += 4;
            Assert.That((ConsistencyLevel)BeConverter.ToInt16(bodyBuffer, offset), Is.EqualTo(expectedSerialConsistencyLevel));
        }

        [Test]
        public void GetRequest_Query_Should_Use_Provided_Keyspace()
        {
            const string keyspace = "my_keyspace";
            var statement = new SimpleStatement("QUERY").SetKeyspace(keyspace);
            var request = RequestHandler.GetRequest(statement, Serializer, new Configuration().DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>]
            //    [<serial_consistency>][<timestamp>][<keyspace>][continuous_paging_options]
            // Skip the query and consistency (2)
            var offset = 4 + statement.QueryString.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            offset +=
                (flags.HasFlag(QueryFlags.WithDefaultTimestamp) ? 8 : 0) +
                (flags.HasFlag(QueryFlags.PageSize) ? 4 : 0) +
                (flags.HasFlag(QueryFlags.WithSerialConsistency) ? 2 : 0);

            var keyspaceLength = BeConverter.ToInt16(bodyBuffer, offset);
            offset += 2;
            Assert.AreEqual(keyspace, Encoding.UTF8.GetString(bodyBuffer, offset, keyspaceLength));
        }

        [Test]
        public void GetRequest_Query_With_Keyspace_On_Lower_Protocol_Version_Should_Ignore_Keyspace()
        {
            var statement = new SimpleStatement("QUERY").SetKeyspace("my_keyspace");
            var serializer = new SerializerManager(ProtocolVersion.V3).GetCurrentSerializer();
            var request = RequestHandler.GetRequest(statement, serializer, new Configuration().DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request, serializer);

            // The query request is composed by:
            // <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>]
            //    [<serial_consistency>][<timestamp>][<keyspace>][continuous_paging_options]
            // Skip the query and consistency (2)
            var offset = 4 + statement.QueryString.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.False(flags.HasFlag(QueryFlags.WithKeyspace));
        }

        [Test]
        public void GetRequest_Query_Should_Use_Serial_Consistency_From_QueryOptions()
        {
            const ConsistencyLevel expectedSerialConsistencyLevel = ConsistencyLevel.LocalSerial;
            var statement = new SimpleStatement("QUERY");

            var config = new Configuration();
            config.QueryOptions.SetSerialConsistencyLevel(expectedSerialConsistencyLevel);

            var request = RequestHandler.GetRequest(statement, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The query request is composed by:
            // <query><consistency><flags><result_page_size><paging_state><serial_consistency><timestamp>
            // Skip the query and consistency (2)
            var offset = 4 + statement.QueryString.Length + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
            // Skip result_page_size (4)
            offset += 4;
            Assert.That((ConsistencyLevel)BeConverter.ToInt16(bodyBuffer, offset), Is.EqualTo(expectedSerialConsistencyLevel));
        }

        [Test]
        public void Prepare_With_Keyspace_Should_Send_Keyspace_And_Flag()
        {
            const string query = "QUERY1";
            const string keyspace = "ks1";
            var request = new PrepareRequest(RequestHandlerTests.Serializer, query, keyspace, null);

            // The request is composed by: <query><flags>[<keyspace>]
            var buffer = GetBodyBuffer(request);

            var queryLength = BeConverter.ToInt32(buffer);
            Assert.AreEqual(query.Length, queryLength);
            var offset = 4 + queryLength;
            var flags = (PrepareFlags)BeConverter.ToInt32(buffer, offset);
            offset += 4;
            Assert.True(flags.HasFlag(PrepareFlags.WithKeyspace));
            var keyspaceLength = BeConverter.ToInt16(buffer, offset);
            offset += 2;
            Assert.AreEqual(keyspace.Length, keyspaceLength);
            Assert.AreEqual(keyspace, Encoding.UTF8.GetString(buffer.Skip(offset).Take(keyspaceLength).ToArray()));
        }

        [Test]
        public void Prepare_With_Keyspace_On_Lower_Protocol_Version_Should_Ignore_Keyspace()
        {
            const string query = "SELECT col1, col2 FROM table1";
            var serializer = new SerializerManager(ProtocolVersion.V2).GetCurrentSerializer();
            var request = new PrepareRequest(serializer, query, "my_keyspace", null);

            // The request only contains the query
            var buffer = GetBodyBuffer(request, serializer);

            var queryLength = BeConverter.ToInt32(buffer);
            Assert.AreEqual(query.Length, queryLength);
            Assert.AreEqual(4 + queryLength, buffer.Length);
        }

        private static byte[] GetBodyBuffer(IRequest request, ISerializer serializer = null)
        {
            if (serializer == null)
            {
                serializer = Serializer;
            }

            var stream = new MemoryStream();
            request.WriteFrame(1, stream, serializer);
            var headerSize = serializer.ProtocolVersion.GetHeaderSize();
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            return bodyBuffer;
        }

        private static void AssertBatchSerialConsistencyLevel(BatchStatement batch, Configuration config, string query,
                                                              ConsistencyLevel expectedSerialConsistencyLevel)
        {
            var request = RequestHandler.GetRequest(batch, Serializer, config.DefaultRequestOptions);
            var bodyBuffer = GetBodyBuffer(request);

            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            // Skip query, n_params and consistency
            var offset = 1 + 2 + 1 + 4 + query.Length + 2 + 2;
            var flags = GetQueryFlags(bodyBuffer, ref offset);
            Assert.True(flags.HasFlag(QueryFlags.WithSerialConsistency));
            Assert.That((ConsistencyLevel)BeConverter.ToInt16(bodyBuffer, offset), Is.EqualTo(expectedSerialConsistencyLevel));
        }

        private static QueryFlags GetQueryFlags(byte[] bodyBuffer, ref int offset, ISerializer serializer = null)
        {
            if (serializer == null)
            {
                serializer = Serializer;
            }
            QueryFlags flags;
            if (serializer.ProtocolVersion.Uses4BytesQueryFlags())
            {
                flags = (QueryFlags)BeConverter.ToInt32(bodyBuffer, offset);
                offset += 4;
            }
            else
            {
                flags = (QueryFlags)bodyBuffer[offset++];
            }

            return flags;
        }

        internal static RetryDecision GetRetryDecisionFromServerError(Exception ex, IExtendedRetryPolicy policy, IStatement statement, Configuration config, int retryCount)
        {
            return RequestExecution.GetRetryDecisionWithReason(RequestError.CreateServerError(ex), policy, statement, config, retryCount).Decision;
        }
        
        internal static RetryDecision GetRetryDecisionFromClientError(Exception ex, IExtendedRetryPolicy policy, IStatement statement, Configuration config, int retryCount)
        {
            return RequestExecution.GetRetryDecisionWithReason(RequestError.CreateClientError(ex, false), policy, statement, config, retryCount).Decision;
        }

        /// <summary>
        /// A timestamp generator that generates empty values
        /// </summary>
        private class NoTimestampGenerator : ITimestampGenerator
        {
            public long Next()
            {
                return long.MinValue;
            }
        }
    }
}