//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Dse.Tasks;
using Dse.Test.Integration.SimulacronAPI.Models.Converters;
using Dse.Test.Integration.SimulacronAPI.Models.Logs;
using Dse.Test.Integration.SimulacronAPI.PrimeBuilder;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Test.Integration.TestClusterManagement.Simulacron
{
    public class SimulacronBase
    {
        public string Id { get; }

        protected SimulacronBase(string id)
        {
            Id = id;
        }

        protected static async Task<JObject> Post(string url, object body)
        {
            var bodyStr = SimulacronBase.GetJsonFromObject(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PostAsync(url, content).ConfigureAwait(false);
                await SimulacronBase.EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JObject.Parse(dataStr);
            }
        }

        private static string GetJsonFromObject(object body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                var jsonSerializerSettings = new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                    {
                        new ConsistencyLevelEnumConverter(),
                        new TupleConverter()
                    }
                };
                bodyStr = JsonConvert.SerializeObject(body, jsonSerializerSettings);
            }
            return bodyStr;
        }

        protected static async Task<JObject> Put(string url, object body)
        {
            var bodyStr = SimulacronBase.GetJsonFromObject(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PutAsync(url, content).ConfigureAwait(false);
                await SimulacronBase.EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                if (string.IsNullOrEmpty(dataStr))
                {
                    return null;
                }
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task<T> Get<T>(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.GetAsync(url).ConfigureAwait(false);
                await SimulacronBase.EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JsonConvert.DeserializeObject<T>(dataStr);
            }
        }

        protected static async Task DeleteAsync(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.DeleteAsync(url).ConfigureAwait(false);
                await SimulacronBase.EnsureSuccessStatusCode(response).ConfigureAwait(false);
            }
        }

        private static async Task EnsureSuccessStatusCode(HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Invalid status code received {response.StatusCode}.{Environment.NewLine}" +
                                    $"{await response.Content.ReadAsStringAsync().ConfigureAwait(false)}");
            }
        }

        public SimulacronClusterLogs GetLogs()
        {
            return TaskHelper.WaitToComplete(GetLogsAsync());
        }

        public Task<SimulacronClusterLogs> GetLogsAsync()
        {
            return SimulacronBase.Get<SimulacronClusterLogs>(GetPath("log"));
        }

        public Task<JObject> PrimeAsync(IPrimeRequest request)
        {
            return SimulacronBase.Post(GetPath("prime"), request.Render());
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
            return SimulacronBase.Get<dynamic>(GetPath("connections"));
        }

        public Task DisableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return SimulacronBase.DeleteAsync(GetPath("listener") + "?after=" + attempts + "&type=" + type);
        }

        public Task<JObject> EnableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Put(GetPath("listener") + "?after=" + attempts + "&type=" + type, null);
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
            return SimulacronBase.DeleteAsync(GetPath("prime"));
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