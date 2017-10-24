//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using Cassandra.Requests;
using Cassandra.Serialization;
using NUnit.Framework;
using QueryFlags = Cassandra.QueryProtocolOptions.QueryFlags;

namespace Cassandra.Tests
{
    [TestFixture]
    public class RequestHandlerTests
    {
        private static readonly Serializer Serializer = new Serializer(ProtocolVersion.MaxSupported);
        
        private static Configuration GetConfig(QueryOptions queryOptions = null)
        {
            return new Configuration(new Policies(),
                new ProtocolOptions(),
                null,
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                queryOptions ?? DefaultQueryOptions,
                new DefaultAddressTranslator());
        }

        private static QueryOptions DefaultQueryOptions => new QueryOptions();

        private static PreparedStatement GetPrepared()
        {
            return new PreparedStatement(null, null, "DUMMY QUERY", null, Serializer);
        }

        [Test]
        public void RequestHandler_GetRequest_SimpleStatement_Default_QueryOptions_Are_Used()
        {
            var stmt = new SimpleStatement("DUMMY QUERY");
            Assert.AreEqual(0, stmt.PageSize);
            Assert.Null(stmt.ConsistencyLevel);
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
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
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig(queryOptions));
            Assert.AreEqual(100, request.PageSize);
            Assert.AreEqual(queryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
            Assert.AreEqual(ConsistencyLevel.Any, request.SerialConsistency);
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
            var request = (QueryRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
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
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
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
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig(queryOptions));
            Assert.AreEqual(100, request.PageSize);
            Assert.AreEqual(queryOptions.GetPageSize(), request.PageSize);
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
            Assert.AreEqual(ConsistencyLevel.Any, request.SerialConsistency);
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
            var request = (ExecuteRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
            Assert.AreEqual(350, request.PageSize);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, request.Consistency);
            Assert.AreEqual(ConsistencyLevel.LocalSerial, request.SerialConsistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_Default_QueryOptions_Are_Used()
        {
            var stmt = new BatchStatement();
            Assert.Null(stmt.ConsistencyLevel);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
            Assert.AreEqual(DefaultQueryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_QueryOptions_Are_Used()
        {
            var stmt = new BatchStatement();
            Assert.Null(stmt.ConsistencyLevel);
            var queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig(queryOptions));
            Assert.AreEqual(queryOptions.GetConsistencyLevel(), request.Consistency);
        }

        [Test]
        public void RequestHandler_GetRequest_BatchStatement_Statement_Level_Settings_Are_Used()
        {
            var stmt = new BatchStatement();
            stmt.SetConsistencyLevel(ConsistencyLevel.EachQuorum);
            Assert.AreEqual(ConsistencyLevel.EachQuorum, stmt.ConsistencyLevel);
            var request = (BatchRequest)RequestHandler.GetRequest(stmt, Serializer, GetConfig());
            Assert.AreEqual(ConsistencyLevel.EachQuorum, request.Consistency);
        }

        [Test]
        public void RequestExecution_GetRetryDecision_Test()
        {
            var config = new Configuration();
            var policy = Policies.DefaultRetryPolicy as IExtendedRetryPolicy;
            var statement = new SimpleStatement("SELECT WILL FAIL");
            //Using default retry policy the decision will always be to rethrow on read/write timeout
            var expected = RetryDecision.RetryDecisionType.Rethrow;
            var decision = RequestExecution.GetRetryDecision(
                new ReadTimeoutException(ConsistencyLevel.Quorum, 1, 2, true), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution.GetRetryDecision(
                new WriteTimeoutException(ConsistencyLevel.Quorum, 1, 2, "SIMPLE"), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution.GetRetryDecision(
                new UnavailableException(ConsistencyLevel.Quorum, 2, 1), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            decision = RequestExecution.GetRetryDecision(
                new Exception(), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);

            //Expecting to retry when a Cassandra node is Bootstrapping/overloaded
            expected = RetryDecision.RetryDecisionType.Retry;
            decision = RequestExecution.GetRetryDecision(
                new OverloadedException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = RequestExecution.GetRetryDecision(
                new IsBootstrappingException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
            decision = RequestExecution.GetRetryDecision(
                new TruncateException(null), policy, statement, config, 0);
            Assert.AreEqual(expected, decision.DecisionType);
        }
        
        [Test]
        public void GetRequest_With_Timestamp_Generator()
        {
            // Timestamp generator should be enabled by default
            var statement = new SimpleStatement("QUERY");
            var config = new Configuration();
            var request = RequestHandler.GetRequest(statement, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The query request is composed by:
            // <query><consistency><flags><result_page_size><timestamp>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            // The remaining length should be 13 = flags (1) + result_page_size (4) + timestamp (8)
            Assert.AreEqual(13, bodyBuffer.Length - offset);
            var flags = (QueryFlags) bodyBuffer[offset];
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.False(flags.HasFlag(QueryFlags.Values));
            Assert.False(flags.HasFlag(QueryFlags.WithPagingState));
            Assert.False(flags.HasFlag(QueryFlags.SkipMetadata));
            Assert.False(flags.HasFlag(QueryFlags.WithSerialConsistency));
            // Skip flags (1) + result_page_size (4)
            offset += 5;
            var timestamp = BeConverter.ToInt64(bodyBuffer, offset);
            var expectedTimestamp = TypeSerializer.SinceUnixEpoch(DateTimeOffset.Now.Subtract(TimeSpan.FromMilliseconds(100))).Ticks / 10;
            Assert.Greater(timestamp, expectedTimestamp);
        }

        [Test]
        public void GetRequest_With_Timestamp_Generator_Empty_Value()
        {
            var statement = new SimpleStatement("QUERY");
            var policies = new Policies(
                Policies.DefaultLoadBalancingPolicy, Policies.DefaultReconnectionPolicy, Policies.DefaultRetryPolicy,
                Policies.DefaultSpeculativeExecutionPolicy, new NoTimestampGenerator());
            var config = new Configuration(
                policies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(), new ClientOptions(),
                NoneAuthProvider.Instance, null, new QueryOptions(), new DefaultAddressTranslator());
            var request = RequestHandler.GetRequest(statement, Serializer.Default, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The query request is composed by:
            // <query><consistency><flags><result_page_size>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                                         .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                                         .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            // The remaining length should be 13 = flags (1) + result_page_size (4)
            Assert.AreEqual(5, bodyBuffer.Length - offset);
            var flags = (QueryFlags) bodyBuffer[offset];
            Assert.False(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            Assert.False(flags.HasFlag(QueryFlags.Values));
            Assert.False(flags.HasFlag(QueryFlags.WithPagingState));
            Assert.False(flags.HasFlag(QueryFlags.SkipMetadata));
            Assert.False(flags.HasFlag(QueryFlags.WithSerialConsistency));
        }

        [Test]
        public void GetRequest_With_Timestamp_Generator_Empty_Value_With_Statement_Timestamp()
        {
            var statement = new SimpleStatement("STATEMENT WITH TIMESTAMP");
            var expectedTimestamp = new DateTimeOffset(2010, 04, 29, 1, 2, 3, 4, TimeSpan.Zero).AddTicks(20);
            statement.SetTimestamp(expectedTimestamp);
            var policies = new Policies(
                Policies.DefaultLoadBalancingPolicy, Policies.DefaultReconnectionPolicy, Policies.DefaultRetryPolicy,
                Policies.DefaultSpeculativeExecutionPolicy, new NoTimestampGenerator());
            var config = new Configuration(
                policies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(), new ClientOptions(),
                NoneAuthProvider.Instance, null, new QueryOptions(), new DefaultAddressTranslator());
            var request = RequestHandler.GetRequest(statement, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The query request is composed by:
            // <query><consistency><flags><result_page_size><timestamp>
            var queryBuffer = BeConverter.GetBytes(statement.QueryString.Length)
                                         .Concat(Encoding.UTF8.GetBytes(statement.QueryString))
                                         .ToArray();
            CollectionAssert.AreEqual(queryBuffer, bodyBuffer.Take(queryBuffer.Length));
            // Skip the query and consistency (2)
            var offset = queryBuffer.Length + 2;
            // The remaining length should be 13 = flags (1) + result_page_size (4) + timestamp (8)
            Assert.AreEqual(13, bodyBuffer.Length - offset);
            var flags = (QueryFlags) bodyBuffer[offset];
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            Assert.True(flags.HasFlag(QueryFlags.PageSize));
            // Skip flags (1) + result_page_size (4)
            offset += 5;
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
            var config = new Configuration(
                Policies.DefaultPolicies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(),
                new ClientOptions(), NoneAuthProvider.Instance, null, new QueryOptions(), new DefaultAddressTranslator());
            var request = RequestHandler.GetRequest(batch, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            CollectionAssert.AreEqual(new byte[] {0xff, 0xff}, bodyBuffer.Skip(1).Take(2));
        }

        [Test]
        public void GetRequest_Batch_With_Timestamp_Generator()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var startDate = DateTimeOffset.Now;
            var config = new Configuration(
                Policies.DefaultPolicies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(),
                new ClientOptions(), NoneAuthProvider.Instance, null, new QueryOptions(),
                new DefaultAddressTranslator());
            var request = RequestHandler<RowSet>.GetRequest(batch, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
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
            var flags = (QueryFlags)bodyBuffer[offset++];
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            var timestamp = TypeSerializer.UnixStart.AddTicks(BeConverter.ToInt64(bodyBuffer, offset) * 10);
            Assert.Greater(timestamp,  startDate);
            Assert.LessOrEqual(timestamp, DateTimeOffset.Now.Add(TimeSpan.FromMilliseconds(100)));
        }
        
        [Test]
        public void GetRequest_Batch_With_Empty_Timestamp_Generator()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var policies = new Policies(
                Policies.DefaultLoadBalancingPolicy, Policies.DefaultReconnectionPolicy, Policies.DefaultRetryPolicy,
                Policies.DefaultSpeculativeExecutionPolicy, new NoTimestampGenerator());
            var config = new Configuration(
                policies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(),
                new ClientOptions(), NoneAuthProvider.Instance, null, new QueryOptions(),
                new DefaultAddressTranslator());
            var request = RequestHandler<RowSet>.GetRequest(batch, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = (QueryFlags)bodyBuffer[offset++];
            Assert.False(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            // No more data
            Assert.AreEqual(bodyBuffer.Length, offset);
        }

        [Test]
        public void GetRequest_Batch_With_Provided_Timestamp()
        {
            var batch = new BatchStatement();
            batch.Add(new SimpleStatement("QUERY"));
            var providedTimestamp = DateTimeOffset.Now;
            batch.SetTimestamp(providedTimestamp);
            var config = new Configuration(
                Policies.DefaultPolicies, new ProtocolOptions(), PoolingOptions.Create(), new SocketOptions(),
                new ClientOptions(), NoneAuthProvider.Instance, null, new QueryOptions(),
                new DefaultAddressTranslator());
            var request = RequestHandler<RowSet>.GetRequest(batch, Serializer, config);
            var stream = new MemoryStream();
            request.WriteFrame(1, stream, Serializer);
            var headerSize = FrameHeader.GetSize(ProtocolVersion.MaxSupported);
            var bodyBuffer = new byte[stream.Length - headerSize];
            stream.Position = headerSize;
            stream.Read(bodyBuffer, 0, bodyBuffer.Length);
            // The batch request is composed by:
            // <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
            var offset = 1 + 2 + 1;
            var queryLength = BeConverter.ToInt32(bodyBuffer, offset);
            Assert.AreEqual(5, queryLength);
            // skip query, n_params and consistency
            offset += 4 + queryLength + 2 + 2;
            var flags = (QueryFlags)bodyBuffer[offset++];
            Assert.True(flags.HasFlag(QueryFlags.WithDefaultTimestamp));
            var timestamp = TypeSerializer.UnixStart.AddTicks(BeConverter.ToInt64(bodyBuffer, offset) * 10);
            Assert.AreEqual(providedTimestamp, timestamp);
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
