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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Dse.Connections;
using Dse.Responses;
using Dse.Serialization;
using Dse.SessionManagement;

namespace Dse.Requests
{
    internal class PrepareHandler : IPrepareHandler
    {
        internal static readonly Logger Logger = new Logger(typeof(PrepareHandler));

        private readonly ISerializer _serializer;
        private readonly IInternalCluster _cluster;
        private readonly IReprepareHandler _reprepareHandler;

        public PrepareHandler(ISerializer serializer, IInternalCluster cluster, IReprepareHandler reprepareHandler)
        {
            _serializer = serializer;
            _cluster = cluster;
            _reprepareHandler = reprepareHandler;
        }

        public async Task<PreparedStatement> Prepare(
            InternalPrepareRequest request, IInternalSession session, IEnumerator<Host> queryPlan)
        {
            var prepareResult = await SendRequestToOneNode(session, queryPlan, request).ConfigureAwait(false);

            if (session.Cluster.Configuration.QueryOptions.IsPrepareOnAllHosts())
            {
                await _reprepareHandler.ReprepareOnAllNodesWithExistingConnections(session, request, prepareResult).ConfigureAwait(false);
            }

            return prepareResult.PreparedStatement;
        }

        private async Task<PrepareResult> SendRequestToOneNode(IInternalSession session, IEnumerator<Host> queryPlan, InternalPrepareRequest request)
        {
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            while (true)
            {
                // It may throw a NoHostAvailableException which we should yield to the caller
                var hostConnectionTuple = await GetNextConnection(session, queryPlan, triedHosts).ConfigureAwait(false);
                var connection = hostConnectionTuple.Item2;
                var host = hostConnectionTuple.Item1;
                try
                {
                    var result = await connection.Send(request).ConfigureAwait(false);
                    return new PrepareResult
                    {
                        PreparedStatement = await GetPreparedStatement(result, request, request.Keyspace ?? connection.Keyspace, session.Cluster).ConfigureAwait(false),
                        TriedHosts = triedHosts,
                        HostAddress = host.Address
                    };
                }
                catch (Exception ex) when (PrepareHandler.CanBeRetried(ex))
                {
                    triedHosts[host.Address] = ex;
                }
            }
        }

        /// <summary>
        /// Determines if the request can be retried on the next node, based on the exception information.
        /// </summary>
        private static bool CanBeRetried(Exception ex)
        {
            return ex is SocketException || ex is OperationTimedOutException || ex is IsBootstrappingException ||
                   ex is OverloadedException || ex is QueryExecutionException;
        }

        private async Task<Tuple<Host, IConnection>> GetNextConnection(IInternalSession session, IEnumerator<Host> queryPlan, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            while ((host = GetNextHost(lbp, queryPlan, out distance)) != null)
            {
                var connection = await RequestHandler.GetConnectionFromHostAsync(host, distance, session, triedHosts).ConfigureAwait(false);
                if (connection != null)
                {
                    return Tuple.Create(host, connection);
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        private Host GetNextHost(ILoadBalancingPolicy lbp, IEnumerator<Host> queryPlan, out HostDistance distance)
        {
            distance = HostDistance.Ignored;
            while (queryPlan.MoveNext())
            {
                var host = queryPlan.Current;
                if (!host.IsUp)
                {
                    continue;
                }
                distance = Cluster.RetrieveAndSetDistance(host, lbp);
                if (distance == HostDistance.Ignored)
                {
                    continue;
                }
                return host;
            }
            return null;
        }

        private async Task<PreparedStatement> GetPreparedStatement(
            Response response, InternalPrepareRequest request, string keyspace, ICluster cluster)
        {
            if (response == null)
            {
                throw new DriverInternalError("Response can not be null");
            }
            var resultResponse = response as ResultResponse;
            if (resultResponse == null)
            {
                throw new DriverInternalError("Excepted ResultResponse, obtained " + response.GetType().FullName);
            }
            var output = resultResponse.Output;
            if (!(output is OutputPrepared))
            {
                throw new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName);
            }
            var prepared = (OutputPrepared)output;
            var ps = new PreparedStatement(prepared.Metadata, prepared.QueryId, prepared.ResultMetadataId,
                request.Query, keyspace, _serializer)
            {
                IncomingPayload = resultResponse.CustomPayload
            };
            await FillRoutingInfo(ps, cluster).ConfigureAwait(false);
            return ps;
        }

        private static async Task FillRoutingInfo(PreparedStatement ps, ICluster cluster)
        {
            var column = ps.Metadata.Columns.FirstOrDefault();
            if (column?.Keyspace == null)
            {
                // The prepared statement does not contain parameters
                return;
            }
            if (ps.Metadata.PartitionKeys != null)
            {
                // The routing indexes where parsed in the prepared response
                if (ps.Metadata.PartitionKeys.Length == 0)
                {
                    // zero-length partition keys means that none of the parameters are partition keys
                    // the partition key is hard-coded.
                    return;
                }
                ps.RoutingIndexes = ps.Metadata.PartitionKeys;
                return;
            }
            try
            {
                const string msgRoutingNotSet = "Routing information could not be set for query \"{0}\"";
                var table = await cluster.Metadata.GetTableAsync(column.Keyspace, column.Table).ConfigureAwait(false);
                if (table == null)
                {
                    Logger.Info(msgRoutingNotSet, ps.Cql);
                    return;
                }
                var routingSet = ps.SetPartitionKeys(table.PartitionKeys);
                if (!routingSet)
                {
                    Logger.Info(msgRoutingNotSet, ps.Cql);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("There was an error while trying to retrieve table metadata for {0}.{1}. {2}",
                    column.Keyspace, column.Table, ex.InnerException);
            }
        }
    }
}