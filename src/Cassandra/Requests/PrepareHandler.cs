//
//      Copyright (C) 2017 DataStax Inc.
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
    internal class PrepareHandler
    {
        private static readonly Logger Logger = new Logger(typeof(PrepareHandler));
        
        private readonly Serializer _serializer;
        private readonly IEnumerator<Host> _queryPlan;

        internal PrepareHandler(Serializer serializer, IEnumerator<Host> queryPlan)
        {
            _serializer = serializer;
            _queryPlan = queryPlan;
        }
        
        /// <summary>
        /// Executes the prepare request on the first host selected by the load balancing policy.
        /// When <see cref="QueryOptions.IsPrepareOnAllHosts"/> is enabled, it prepares on the rest of the hosts in
        /// parallel.
        /// </summary>
        internal static async Task<PreparedStatement> Prepare(IInternalSession session, Serializer serializer, PrepareRequest request)
        {
            var cluster = session.InternalCluster;
            var lbp = cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            var handler = new PrepareHandler(serializer, lbp.NewQueryPlan(session.Keyspace, null).GetEnumerator());
            var ps = await handler.Prepare(request, session, null).ConfigureAwait(false);
            var psAdded = cluster.PreparedQueries.GetOrAdd(ps.Id, ps);
            if (ps != psAdded)
            {
                Logger.Warning("Re-preparing already prepared query is generally an anti-pattern and will likely " +
                               "affect performance. Consider preparing the statement only once. Query='{0}'", ps.Cql);
                ps = psAdded;
            }
            var prepareOnAllHosts = cluster.Configuration.DefaultRequestOptions.PrepareOnAllHosts;
            if (!prepareOnAllHosts)
            {
                return ps;
            }
            await handler.PrepareOnTheRestOfTheNodes(request, session).ConfigureAwait(false);
            return ps;
        }

        internal static async Task PrepareAllQueries(Host host, ICollection<PreparedStatement> preparedQueries,
                                                     IEnumerable<IInternalSession> sessions)
        {
            if (preparedQueries.Count == 0)
            {
                return;
            }
            // Get the first connection for that host, in any of the existings connection pool
            var connection = sessions.SelectMany(s => s.GetExistingPool(host.Address)?.ConnectionsSnapshot)
                                     .FirstOrDefault();
            if (connection == null)
            {
                Logger.Info($"Could not re-prepare queries on {host.Address} as there wasn't an open connection to" +
                            " the node");
                return;
            }
            Logger.Info($"Re-preparing {preparedQueries.Count} queries on {host.Address}");
            var tasks = new List<Task>(preparedQueries.Count);
            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                foreach (var query in preparedQueries.Select(ps => ps.Cql))
                {
                    var request = new PrepareRequest(query);
                    await semaphore.WaitAsync().ConfigureAwait(false);

                    async Task SendSingle()
                    {
                        try
                        {
                            await connection.Send(request).ConfigureAwait(false);
                        }
                        finally
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            // There is no risk of being disposed as the list of tasks is awaited upon below
                            semaphore.Release();
                        }
                    }

                    tasks.Add(Task.Run(SendSingle));
                }
                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.Error($"There was an error when re-preparing queries on {host.Address}", ex);
                }
            }
        }

        private async Task<PreparedStatement> Prepare(PrepareRequest request, IInternalSession session,
                                                      Dictionary<IPEndPoint, Exception> triedHosts)
        {
            if (triedHosts == null)
            {
                triedHosts = new Dictionary<IPEndPoint, Exception>();
            }
            // It may throw a NoHostAvailableException which we should yield to the caller
            var connection = await GetNextConnection(session, triedHosts)
                .ConfigureAwait(false);
            Response response;
            try
            {
                response = await connection.Send(request).ConfigureAwait(false);
            }
            catch (Exception ex) when (CanBeRetried(ex))
            {
                triedHosts[connection.Address] = ex;
                return await Prepare(request, session, triedHosts).ConfigureAwait(false);
            }
            return await GetPreparedStatement(response, request, connection.Keyspace, session.Cluster)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Determines if the request can be retried on the next node, based on the exception information.
        /// </summary>
        private static bool CanBeRetried(Exception ex)
        {
            return ex is SocketException || ex is OperationTimedOutException || ex is IsBootstrappingException ||
                   ex is OverloadedException || ex is QueryExecutionException;
        }

        private async Task PrepareOnTheRestOfTheNodes(PrepareRequest request, IInternalSession session)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            var tasks = new List<Task>();
            var triedHosts = new Dictionary<IPEndPoint, Exception>();
            while ((host = GetNextHost(lbp, out distance)) != null)
            {
                var connection = await RequestHandler
                    .GetConnectionFromHostAsync(host, distance, session, triedHosts).ConfigureAwait(false);
                if (connection == null)
                {
                    continue;
                }
                // For each valid connection, send a the request in parallel
                tasks.Add(connection.Send(request));
            }
            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                // Don't consider individual failures
            }
        }
        
        private async Task<PreparedStatement> GetPreparedStatement(Response response, PrepareRequest request,
                                                                   string keyspace, ICluster cluster)
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

        private async Task<IConnection> GetNextConnection(IInternalSession session, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            while ((host = GetNextHost(lbp, out distance)) != null)
            {
                var connection = await RequestHandler
                    .GetConnectionFromHostAsync(host, distance, session, triedHosts).ConfigureAwait(false);
                if (connection != null)
                {
                    return connection;
                }
            }
            throw new NoHostAvailableException(triedHosts);
        }

        private Host GetNextHost(ILoadBalancingPolicy lbp, out HostDistance distance)
        {
            distance = HostDistance.Ignored;
            while (_queryPlan.MoveNext())
            {
                var host = _queryPlan.Current;
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
    }
}