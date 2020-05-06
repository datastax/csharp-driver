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
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.Tasks;

using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronBase
    {
        private readonly SimulacronManager _simulacronManager;

        public string Id { get; }

        protected SimulacronBase(string id, SimulacronManager simulacronManager)
        {
            _simulacronManager = simulacronManager;
            Id = id;
        }

        protected Task<JObject> PostAsync(string url, object body)
        {
            return SimulacronBase.PostAsync(_simulacronManager, url, body);
        }

        protected Task<JObject> PutAsync(string url, object body)
        {
            return PutAsync(_simulacronManager, url, body);
        }

        protected Task<T> GetAsync<T>(string url)
        {
            return GetAsync<T>(_simulacronManager, url);
        }

        protected Task DeleteAsync(string url)
        {
            return DeleteAsync(_simulacronManager, url);
        }

        protected static Task<JObject> PostAsync(SimulacronManager simulacronManager, string url, object body)
        {
            return simulacronManager.Post(url, body);
        }

        protected static Task<JObject> PutAsync(SimulacronManager simulacronManager, string url, object body)
        {
            return simulacronManager.PutAsync(url, body);
        }

        protected static Task<T> GetAsync<T>(SimulacronManager simulacronManager, string url)
        {
            return simulacronManager.GetAsync<T>(url);
        }

        protected static Task DeleteAsync(SimulacronManager simulacronManager, string url)
        {
            return simulacronManager.DeleteAsync(url);
        }

        public SimulacronClusterLogs GetLogs()
        {
            return TaskHelper.WaitToComplete(GetLogsAsync());
        }

        public Task<SimulacronClusterLogs> GetLogsAsync()
        {
            return GetAsync<SimulacronClusterLogs>(GetPath("log"));
        }

        public Task<JObject> PrimeAsync(IPrimeRequest request)
        {
            return PostAsync(GetPath("prime"), request.Render());
        }

        public JObject Prime(IPrimeRequest request)
        {
            return TaskHelper.WaitToComplete(PrimeAsync(request));
        }

        protected string GetPath(string endpoint)
        {
            return "/" + endpoint + "/" + Id;
        }

        public Task<dynamic> GetConnectionsAsync()
        {
            return GetAsync<dynamic>(GetPath("connections"));
        }

        public Task DisableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return DeleteAsync(GetPath("listener") + "?after=" + attempts + "&type=" + type);
        }

        public Task<JObject> EnableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return PutAsync(GetPath("listener") + "?after=" + attempts + "&type=" + type, null);
        }

        public Task PauseReadsAsync()
        {
            return PutAsync(GetPath("pause-reads"), null);
        }

        public Task ResumeReadsAsync()
        {
            return DeleteAsync(GetPath("pause-reads"));
        }

        public IList<RequestLog> GetQueries(string query, QueryType? queryType = QueryType.Query)
        {
            return TaskHelper.WaitToComplete(GetQueriesAsync(query, queryType));
        }

        public async Task<IList<RequestLog>> GetQueriesAsync(string query, QueryType? queryType = QueryType.Query)
        {
            var response = await GetLogsAsync().ConfigureAwait(false);
            var dcInfo = response?.DataCenters;
            if (dcInfo == null)
            {
                return new List<RequestLog>();
            }
            return dcInfo
                   .Select(dc => dc.Nodes)
                   .Where(nodes => nodes != null)
                   .SelectMany(nodes => nodes)
                   .Where(n => n.Queries != null)
                   .SelectMany(n => n.Queries)
                   .Where(q => (q.Type == queryType || queryType == null) && (q.Query == query || query == null))
                   .ToArray();
        }

        public void PrimeDelete()
        {
            TaskHelper.WaitToComplete(PrimeDeleteAsync());
        }

        public Task PrimeDeleteAsync()
        {
            return DeleteAsync(GetPath("prime"));
        }

        public JObject PrimeFluent(Func<IPrimeRequestBuilder, IThenFluent> builder)
        {
            return TaskHelper.WaitToComplete(PrimeFluentAsync(builder));
        }

        public Task<JObject> PrimeFluentAsync(Func<IPrimeRequestBuilder, IThenFluent> builder)
        {
            var prime = SimulacronBase.PrimeBuilder();
            return builder(prime).ApplyAsync(this);
        }

        public static IPrimeRequestBuilder PrimeBuilder()
        {
            return new PrimeRequestBuilder();
        }
    }
}