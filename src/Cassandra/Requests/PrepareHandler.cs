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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.ExecutionProfiles;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal class PrepareHandler : IPrepareHandler
    {
        internal static readonly Logger Logger = new Logger(typeof(PrepareHandler));
        
        private readonly Serializer _serializer;
        private readonly IEnumerator<Host> _queryPlan;

        public PrepareHandler(Serializer serializer, IEnumerator<Host> queryPlan)
        {
            _serializer = serializer;
            _queryPlan = queryPlan;
        }
        
        public async Task<PreparedStatement> Prepare(
            InternalPrepareRequest request, IInternalSession session)
        {
            if (session.Cluster.Configuration.QueryOptions.IsPrepareOnAllHosts())
            {
                return await SendRequestToAllNodesWithExistingConnections(session, request).ConfigureAwait(false);
            }

            return await SendRequestToOneNode(session, request).ConfigureAwait(false);
        }

        /// <summary>
        /// Determines if the request can be retried on the next node, based on the exception information.
        /// </summary>
        private static bool CanBeRetried(Exception ex)
        {
            return ex is SocketException || ex is OperationTimedOutException || ex is IsBootstrappingException ||
                   ex is OverloadedException || ex is QueryExecutionException;
        }
                
        private async Task<PreparedStatement> GetPreparedStatement(Response response, InternalPrepareRequest request,
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

        private async Task<PreparedStatement> CreateConnectionIfNeededAndSendRequest(IInternalSession session, InternalPrepareRequest request)
        {
            var triedHosts = new Dictionary<IPEndPoint, Exception>();
            
            while(true)
            {
                // It may throw a NoHostAvailableException which we should yield to the caller
                var connection = await GetNextConnection(session, triedHosts).ConfigureAwait(false);
                try
                {
                    var result =  await connection.Send(request).ConfigureAwait(false);
                    return await GetPreparedStatement(result, request, connection.Keyspace, session.Cluster).ConfigureAwait(false);
                }
                catch (Exception ex) when (CanBeRetried(ex))
                {
                    triedHosts[connection.Address] = ex;
                }
            }
        }

        private async Task<PreparedStatement> SendRequestToOneNode(IInternalSession session, InternalPrepareRequest request)
        {
            var connectionFound = false;
            var triedHosts = new Dictionary<IPEndPoint, Exception>();
            var hosts = session.Cluster.AllHosts();

            foreach (var host in hosts)
            {
                // Get the first connection for that host
                var connection = session.GetExistingPool(host.Address)?.ConnectionsSnapshot.FirstOrDefault();
                if (connection == null)
                {
                    PrepareHandler.Logger.Info(
                        $"Did not prepare request on {host.Address} as there wasn't an open connection to the node");
                    continue;
                }

                connectionFound = true;
                PrepareHandler.Logger.Info($"Preparing request on {host.Address}");
                try
                {
                    var result =  await connection.Send(request).ConfigureAwait(false);
                    return await GetPreparedStatement(result, request, connection.Keyspace, session.Cluster).ConfigureAwait(false);
                }
                catch (Exception ex) when (PrepareHandler.CanBeRetried(ex))
                {
                    triedHosts[connection.Address] = ex;
                }
            }

            if (!connectionFound)
            {
                return await CreateConnectionIfNeededAndSendRequest(session, request).ConfigureAwait(false);
            }
            
            throw new NoHostAvailableException(triedHosts);
        }

        private async Task<PreparedStatement> SendRequestToAllNodesWithExistingConnections(IInternalSession session, InternalPrepareRequest request)
        {
            var triedHosts = new ConcurrentDictionary<IPEndPoint, Exception>();

            var hosts = session.Cluster.AllHosts();
            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                var tasks = new List<Task<Tuple<Response,IConnection>>>(hosts.Count);
                foreach (var host in hosts)
                {
                    // Get the first connection for that host
                    var connection = session.GetExistingPool(host.Address)?.ConnectionsSnapshot.FirstOrDefault();
                    if (connection == null)
                    {
                        PrepareHandler.Logger.Info(
                            $"Did not prepare request on {host.Address} as there wasn't an open connection to the node");
                        continue;
                    }

                    PrepareHandler.Logger.Info($"Preparing request on {host.Address}");
                    await semaphore.WaitAsync().ConfigureAwait(false);
                    tasks.Add(SendSingleRequest(connection, request, semaphore, triedHosts));
                }

                if (tasks.Count == 0)
                {
                    throw new NoHostAvailableException("No connections available to prepare the request.");
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    var result = tasks.First().Result;
                    return await GetPreparedStatement(result.Item1, request, result.Item2.Keyspace, session.Cluster).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var completedTasks = tasks.Where(t => t.IsCompleted && !t.IsFaulted && !t.IsCanceled).ToList();
                    if (completedTasks.Count > 0)
                    {
                        PrepareHandler.Logger.Warning("Prepare did not succeed on all hosts. There was one or more errors when preparing the query on all hosts.", ex);
                        var result = completedTasks.First().Result;
                        return await GetPreparedStatement(result.Item1, request, result.Item2.Keyspace, session.Cluster).ConfigureAwait(false);
                    }

                    PrepareHandler.Logger.Error("There was one or more errors when preparing the query on all hosts.", ex);
                    throw new NoHostAvailableException(triedHosts.ToDictionary(kvp => kvp.Key, kvp => kvp.Value));
                }
            }
        }

        private async Task<Tuple<Response, IConnection>> SendSingleRequest(IConnection connection, IRequest request, SemaphoreSlim sem, ConcurrentDictionary<IPEndPoint, Exception> triedHosts)
        {
            try
            {
                return new Tuple<Response, IConnection>(await connection.Send(request).ConfigureAwait(false), connection);
            }
            catch (Exception ex)
            {
                triedHosts.AddOrUpdate(connection.Address, ex, (point, exception) => exception);
                throw;
            }
            finally
            {
                sem.Release();
            }
        }
    }
}