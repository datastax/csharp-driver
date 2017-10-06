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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Cassandra.Responses;
using Cassandra.Serialization;

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
        internal static async Task<PreparedStatement> Send(Session session, Serializer serializer, 
                                                           PrepareRequest request)
        {
            // The cast to Cluster class is safe as we are using the Session concrete implementation as parameter
            var cluster = ((Cluster) session.Cluster);
            var lbp = cluster.Configuration.Policies.LoadBalancingPolicy;
            var handler = new PrepareHandler(serializer, lbp.NewQueryPlan(session.Keyspace, null).GetEnumerator());
            var ps = await handler.Prepare(request, session, null).ConfigureAwait(false);
            var psAdded = cluster.PreparedQueries.GetOrAdd(ps.Id, ps);
            if (ps != psAdded)
            {
                Logger.Warning("Re-preparing already prepared query is generally an anti-pattern and will likely " +
                               "affect performance. Consider preparing the statement only once. Query='{0}'", ps.Cql);
                ps = psAdded;
            }
            var prepareOnAllHosts = cluster.Configuration.QueryOptions.IsPrepareOnAllHosts();
            if (!prepareOnAllHosts)
            {
                return ps;
            }
            await handler.PrepareOnTheRestOfTheNodes(request, session).ConfigureAwait(false);
            return ps;
        }

        internal static Task PrepareAllQueries(Cluster cluster, Host host)
        {
            foreach (var ps in cluster.PreparedQueries.Values)
            {
                //TODO
            }
            throw new NotImplementedException();
        }

        private async Task<PreparedStatement> Prepare(PrepareRequest request, Session session,
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
            catch (SocketException ex)
            {
                triedHosts[connection.Address] = ex;
                return await Prepare(request, session, triedHosts).ConfigureAwait(false);
            }
            return GetPreparedStatement(response, request, connection.Keyspace);
        }

        private async Task PrepareOnTheRestOfTheNodes(PrepareRequest request, Session session)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.Policies.LoadBalancingPolicy;
            var tasks = new List<Task>();
            var triedHosts = new Dictionary<IPEndPoint, Exception>();
            while ((host = GetNextHost(lbp, out distance)) != null)
            {
                var connection = await RequestHandler<RowSet>
                    .GetConnectionFromHost(host, distance, session, triedHosts).ConfigureAwait(false);
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
        
        private PreparedStatement GetPreparedStatement(Response response, PrepareRequest request, string keyspace)
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
            return new PreparedStatement(prepared.Metadata, prepared.QueryId, request.Query, keyspace, _serializer)
            {
                IncomingPayload = resultResponse.CustomPayload
            };
        }

        private async Task<Connection> GetNextConnection(Session session, Dictionary<IPEndPoint, Exception> triedHosts)
        {
            Host host;
            HostDistance distance;
            var lbp = session.Cluster.Configuration.Policies.LoadBalancingPolicy;
            while ((host = GetNextHost(lbp, out distance)) != null)
            {
                var connection = await RequestHandler<RowSet>
                    .GetConnectionFromHost(host, distance, session, triedHosts).ConfigureAwait(false);
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