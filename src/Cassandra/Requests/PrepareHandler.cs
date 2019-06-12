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
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal class PrepareHandler : IPrepareHandler
    {
        internal static readonly Logger Logger = new Logger(typeof(PrepareHandler));

        private readonly Serializer _serializer;

        public PrepareHandler(Serializer serializer)
        {
            _serializer = serializer;
        }

        public async Task<PreparedStatement> Prepare(
            InternalPrepareRequest request, IInternalSession session, IEnumerator<Host> queryPlan)
        {
            var prepareResult = await SendRequestToOneNode(session, queryPlan, request).ConfigureAwait(false);

            if (session.Cluster.Configuration.QueryOptions.IsPrepareOnAllHosts())
            {
                await SendRequestToAllNodesWithExistingConnections(session, request, prepareResult).ConfigureAwait(false);
            }

            return prepareResult.PreparedStatement;
        }

        private async Task<PrepareResult> SendRequestToOneNode(IInternalSession session, IEnumerator<Host> queryPlan, InternalPrepareRequest request)
        {
            var triedHosts = new Dictionary<IPEndPoint, Exception>();

            while (true)
            {
                // It may throw a NoHostAvailableException which we should yield to the caller
                var connection = await GetNextConnection(session, queryPlan, triedHosts).ConfigureAwait(false);
                try
                {
                    var result = await connection.Send(request).ConfigureAwait(false);
                    return new PrepareResult
                    {
                        PreparedStatement = await GetPreparedStatement(result, request, connection.Keyspace, session.Cluster).ConfigureAwait(false),
                        TriedHosts = triedHosts,
                        HostAddress = connection.Address
                    };
                }
                catch (Exception ex) when (PrepareHandler.CanBeRetried(ex))
                {
                    triedHosts[connection.Address] = ex;
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

        private async Task<IConnection> GetNextConnection(IInternalSession session, IEnumerator<Host> queryPlan, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            while ((host = GetNextHost(lbp, queryPlan, out distance)) != null)
            {
                var connection = await RequestHandler.GetConnectionFromHostAsync(host, distance, session, triedHosts).ConfigureAwait(false);
                if (connection != null)
                {
                    return connection;
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
                distance = Cluster.RetrieveDistance(host, lbp);
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
            var ps = new PreparedStatement(prepared.Metadata, prepared.QueryId, request.Query, keyspace, _serializer)
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

        /// <summary>
        /// Sends the prepare request to all nodes have have an existing open connection. Will not attempt to send the request to hosts that were tried before (successfully or not).
        /// </summary>
        /// <param name="session"></param>
        /// <param name="request"></param>
        /// <param name="prepareResult">The result of the prepare request on the first node.</param>
        /// <returns></returns>
        private async Task SendRequestToAllNodesWithExistingConnections(IInternalSession session, InternalPrepareRequest request, PrepareResult prepareResult)
        {
            var pools = session.GetPools().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            if (pools.Count == 0)
            {
                PrepareHandler.Logger.Warning("Could not prepare query on all hosts because there are no connection pools.");
                return;
            }

            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                var tasks = new List<Task>(pools.Count);
                foreach (var poolKvp in pools)
                {
                    if (poolKvp.Key.Equals(prepareResult.HostAddress))
                    {
                        continue;
                    }

                    if (prepareResult.TriedHosts.ContainsKey(poolKvp.Key))
                    {
                        PrepareHandler.Logger.Warning(
                            $"An error occured while attempting to prepare query on {{0}}:{Environment.NewLine}{{1}}", 
                            poolKvp.Key, 
                            prepareResult.TriedHosts[poolKvp.Key]);
                        continue;
                    }

                    await semaphore.WaitAsync().ConfigureAwait(false);
                    tasks.Add(SendSingleRequest(poolKvp, request, semaphore));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        private async Task SendSingleRequest(KeyValuePair<IPEndPoint, IHostConnectionPool> poolKvp, IRequest request, SemaphoreSlim sem)
        {
            try
            {
                var connection = poolKvp.Value.BorrowExistingConnection();
                await connection.Send(request).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                PrepareHandler.Logger.Warning($"An error occured while attempting to prepare query on {{0}}:{Environment.NewLine}{{1}}", poolKvp.Key, ex);
            }
            finally
            {
                sem.Release();
            }
        }

        private class PrepareResult
        {
            public PreparedStatement PreparedStatement { get; set; }

            public IDictionary<IPEndPoint, Exception> TriedHosts { get; set; }

            public IPEndPoint HostAddress { get; set; }
        }
    }
}