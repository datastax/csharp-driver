//
//       Copyright (C) DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Cassandra.DataStax.Graph;
using Cassandra.ExecutionProfiles;
using Cassandra.Serialization.Graph;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    /// <inheritdoc />
    internal class GraphRequestHandler : IGraphRequestHandler
    {
        private static readonly Logger Logger = new Logger(typeof(GraphRequestHandler));

        private readonly IInternalSession _session;
        private readonly IGraphTypeSerializerFactory _graphTypeSerializerFactory;

        public GraphRequestHandler(IInternalSession session, IGraphTypeSerializerFactory graphTypeSerializerFactory)
        {
            _session = session;
            _graphTypeSerializerFactory = graphTypeSerializerFactory;
        }

        public Task<GraphResultSet> SendAsync(IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            return ExecuteGraphAsync(graphStatement, requestOptions);
        }

        private async Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            var graphOptions = requestOptions.GraphOptions;

            if (graphStatement.GraphProtocolVersion == null && requestOptions.GraphOptions.GraphProtocolVersion == null)
            {
                var version = _graphTypeSerializerFactory.GetDefaultGraphProtocol(
                    _session, graphStatement, requestOptions.GraphOptions);
                graphOptions = new GraphOptions(graphOptions, version);
            }

            var conversionResult = GetIStatement(graphStatement, graphOptions);

            var stmt = conversionResult.Statement;
            var serializer = conversionResult.Serializer;

            await GetAnalyticsPrimary(stmt, graphStatement, requestOptions).ConfigureAwait(false);
            var rs = await _session.ExecuteAsync(stmt, requestOptions).ConfigureAwait(false);
            return CreateGraphResultSet(rs, serializer);
        }

        private async Task<IStatement> GetAnalyticsPrimary(
            IStatement statement, IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            if (!(statement is TargettedSimpleStatement) || !requestOptions.GraphOptions.IsAnalyticsQuery(graphStatement))
            {
                return statement;
            }

            var targetedSimpleStatement = (TargettedSimpleStatement)statement;

            RowSet rs;
            try
            {
                rs = await _session.ExecuteAsync(
                    new SimpleStatement("CALL DseClientTool.getAnalyticsGraphServer()"), requestOptions).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                GraphRequestHandler.Logger.Verbose("Error querying graph analytics server, query will not be routed optimally: {0}", ex);
                return statement;
            }

            return AdaptRpcPrimaryResult(rs, targetedSimpleStatement);
        }

        private IStatement AdaptRpcPrimaryResult(RowSet rowSet, TargettedSimpleStatement statement)
        {
            var row = rowSet.FirstOrDefault();
            if (row == null)
            {
                GraphRequestHandler.Logger.Verbose(
                    "Empty response querying graph analytics server, query will not be routed optimally");
                return statement;
            }
            var resultField = row.GetValue<IDictionary<string, string>>("result");
            if (resultField == null || !resultField.ContainsKey("location") || resultField["location"] == null)
            {
                GraphRequestHandler.Logger.Verbose(
                    "Could not extract graph analytics server location from RPC, query will not be routed optimally");
                return statement;
            }
            var location = resultField["location"];
            var hostName = location.Substring(0, location.LastIndexOf(':'));
            var address = _session.Cluster.Configuration.AddressTranslator.Translate(
                new IPEndPoint(IPAddress.Parse(hostName), _session.Cluster.Configuration.ProtocolOptions.Port));
            var host = _session.Cluster.GetHost(address);
            statement.PreferredHost = host;
            return statement;
        }

        private GraphResultSet CreateGraphResultSet(RowSet rs, IGraphTypeSerializer serializer)
        {
            return GraphResultSet.CreateNew(
                rs,
                serializer.GraphProtocol,
                serializer.GetGraphRowParser());
        }

        private ConvertedStatementResult GetIStatement(IGraphStatement graphStmt, GraphOptions options)
        {
            var graphProtocol = graphStmt.GraphProtocolVersion ?? options.GraphProtocolVersion;

            if (graphProtocol == null)
            {
                throw new DriverInternalError("Unable to determine graph protocol version. This is a bug, please report.");
            }

            // Existing graph statement implementations of this method are empty
            // but it's part of the public interface definition
            var stmt = graphStmt.ToIStatement(options);
            if (stmt != null)
            {
                return new ConvertedStatementResult
                {
                    Serializer = _graphTypeSerializerFactory.CreateSerializer(
                        _session, null, null, graphProtocol.Value, true),
                    Statement = stmt
                };
            }

            return ConvertGraphStatement(graphStmt, options, graphProtocol.Value);
        }

        private ConvertedStatementResult ConvertGraphStatement(
            IGraphStatement graphStmt, GraphOptions options, GraphProtocol graphProtocol)
        {
            string jsonParams;
            string query;
            IGraphTypeSerializer serializer;

            if (graphStmt is SimpleGraphStatement simpleGraphStatement)
            {
                serializer = _graphTypeSerializerFactory.CreateSerializer(_session, null, null, graphProtocol, true);
                query = simpleGraphStatement.Query;
                if (simpleGraphStatement.ValuesDictionary != null)
                {
                    jsonParams = serializer.ToDb(simpleGraphStatement.ValuesDictionary);
                }
                else if (simpleGraphStatement.Values != null)
                {
                    jsonParams = serializer.ToDb(Utils.GetValues(simpleGraphStatement.Values));
                }
                else
                {
                    jsonParams = null;
                }
            }
            else if (graphStmt is FluentGraphStatement fluentGraphStatement)
            {
                serializer = _graphTypeSerializerFactory.CreateSerializer(
                    _session,
                    fluentGraphStatement.CustomDeserializers,
                    fluentGraphStatement.CustomSerializers,
                    graphProtocol,
                    fluentGraphStatement.DeserializeGraphNodes);
                query = serializer.ToDb(fluentGraphStatement.QueryBytecode);
                jsonParams = null;
            }
            else
            {
                throw new NotSupportedException("Statement of type " + graphStmt.GetType().FullName + " not supported");
            }

            IStatement stmt = jsonParams != null
                ? new TargettedSimpleStatement(query, jsonParams)
                : new TargettedSimpleStatement(query);

            //Set Cassandra.Statement properties
            if (graphStmt.Timestamp != null)
            {
                stmt.SetTimestamp(graphStmt.Timestamp.Value);
            }
            var readTimeout = graphStmt.ReadTimeoutMillis != 0 ? graphStmt.ReadTimeoutMillis : options.ReadTimeoutMillis;
            if (readTimeout <= 0)
            {
                // Infinite (-1) is not supported in the core driver, set an arbitrarily large int
                readTimeout = int.MaxValue;
            }

            stmt = stmt
                   .SetIdempotence(false)
                   .SetConsistencyLevel(graphStmt.ConsistencyLevel)
                   .SetReadTimeoutMillis(readTimeout)
                   .SetOutgoingPayload(options.BuildPayload(graphStmt));

            return new ConvertedStatementResult
            {
                Serializer = serializer,
                Statement = stmt
            };
        }

        private struct ConvertedStatementResult
        {
            public IStatement Statement { get; set; }

            public IGraphTypeSerializer Serializer { get; set; }
        }
    }
}