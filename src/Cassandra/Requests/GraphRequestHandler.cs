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
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal class GraphRequestHandler : IGraphRequestHandler
    {
        private static readonly Logger Logger = new Logger(typeof(GraphRequestHandler));

        private readonly IInternalSession _session;
        private readonly IGraphProtocolResolver _graphProtocolResolver;

        public GraphRequestHandler(IInternalSession session, IGraphProtocolResolver graphProtocolResolver)
        {
            _session = session;
            _graphProtocolResolver = graphProtocolResolver;
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
                var version = _graphProtocolResolver.GetDefaultGraphProtocol(
                    _session, graphStatement, requestOptions.GraphOptions);
                graphOptions = new GraphOptions(graphOptions, version);
            }

            var stmt = graphStatement.ToIStatement(graphOptions) ?? ConvertToIStatement(graphStatement, graphOptions);

            await GetAnalyticsPrimary(stmt, graphStatement, requestOptions).ConfigureAwait(false);
            var rs = await _session.ExecuteAsync(stmt, requestOptions).ConfigureAwait(false);
            return CreateGraphResultSet(rs, graphStatement, graphOptions);
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

        private GraphResultSet CreateGraphResultSet(RowSet rs, IGraphStatement statement, GraphOptions options)
        {
            var graphProtocolVersion = statement.GraphProtocolVersion ?? options.GraphProtocolVersion;

            if (graphProtocolVersion == null)
            {
                throw new DriverInternalError("Unable to determine graph protocol version. This is a bug, please report.");
            }

            return GraphResultSet.CreateNew(
                rs,
                graphProtocolVersion.Value,
                _graphProtocolResolver.GetGraphRowParser(graphProtocolVersion.Value));
        }

        private IStatement ConvertToIStatement(IGraphStatement graphStmt, GraphOptions options)
        {
            if (!(graphStmt is SimpleGraphStatement graphStatement))
            {
                throw new NotSupportedException("Statement of type " + graphStmt.GetType().FullName + " not supported");
            }

            IDictionary<string, object> parameters = null;
            if (graphStatement.ValuesDictionary != null)
            {
                parameters = graphStatement.ValuesDictionary;
            }
            else if (graphStatement.Values != null)
            {
                parameters = Utils.GetValues(graphStatement.Values);
            }

            var graphProtocol = graphStatement.GraphProtocolVersion ?? options.GraphProtocolVersion;

            if (graphProtocol == null)
            {
                throw new DriverInternalError("Unable to determine graph protocol version. This is a bug, please report.");
            }

            IStatement stmt;
            if (parameters != null)
            {
                var jsonParams = _graphProtocolResolver.GetParametersSerializer(graphProtocol.Value).Invoke(parameters);
                stmt = new TargettedSimpleStatement(graphStatement.Query, jsonParams);
            }
            else
            {
                stmt = new TargettedSimpleStatement(graphStatement.Query);
            }
            //Set Cassandra.Statement properties
            if (graphStatement.Timestamp != null)
            {
                stmt.SetTimestamp(graphStatement.Timestamp.Value);
            }
            var readTimeout = graphStatement.ReadTimeoutMillis != 0 ? graphStatement.ReadTimeoutMillis : options.ReadTimeoutMillis;
            if (readTimeout <= 0)
            {
                // Infinite (-1) is not supported in the core driver, set an arbitrarily large int
                readTimeout = int.MaxValue;
            }
            return stmt
                .SetIdempotence(false)
                .SetConsistencyLevel(graphStatement.ConsistencyLevel)
                .SetReadTimeoutMillis(readTimeout)
                .SetOutgoingPayload(options.BuildPayload(graphStatement));
        }
    }

    internal interface IGraphRequestHandler
    {
        Task<GraphResultSet> SendAsync(IGraphStatement graphStatement, IRequestOptions requestOptions);
    }
}