using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronBase
    {
        public string Id { get; }

        protected SimulacronBase(string id)
        {
            Id = id;
        }

        protected static async Task<dynamic> Post(string url, dynamic body)
        {
            var bodyStr = GetJsonFromDynamic(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PostAsync(url, content).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    // Get the error message
                    throw new InvalidOperationException(await response.Content.ReadAsStringAsync()
                                                                      .ConfigureAwait(false));
                }
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JObject.Parse(dataStr);
            }
        }

        private static string GetJsonFromDynamic(dynamic body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                bodyStr = JObject.FromObject(body).ToString();
            }
            return bodyStr;
        }

        protected static async Task<dynamic> Put(string url, dynamic body)
        {
            var bodyStr = GetJsonFromDynamic(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PutAsync(url, content).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                if (string.IsNullOrEmpty(dataStr))
                {
                    return null;
                }
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task<dynamic> Get(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.GetAsync(url).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task Delete(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.DeleteAsync(url).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
            }
        }

        public dynamic GetLogs()
        {
            return TaskHelper.WaitToComplete(GetLogsAsync());
        }

        public Task<dynamic> GetLogsAsync()
        {
            return Get(GetPath("log"));
        }

        public dynamic Prime(dynamic body)
        {
            Task<dynamic> task = Post(GetPath("prime"), body);
            return TaskHelper.WaitToComplete(task);
        }

        protected string GetPath(string endpoint)
        {
            return "/" + endpoint + "/" + Id;
        }

        public dynamic GetConnections()
        {
            return TaskHelper.WaitToComplete(Get(GetPath("connections")));
        }

        public Task DisableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Delete(GetPath("listener") + "?after=" + attempts + "&type=" + type);
        }

        public Task<dynamic> EnableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Put(GetPath("listener") + "?after=" + attempts + "&type=" + type, null);
        }

        public IList<dynamic> GetQueries(string query, string queryType = "QUERY")
        {
            return TaskHelper.WaitToComplete(GetQueriesAsync(query, queryType));
        }

        public async Task<IList<dynamic>> GetQueriesAsync(string query, string queryType = "QUERY")
        {
            var response = await GetLogsAsync().ConfigureAwait(false);
            IEnumerable<dynamic> dcInfo = response?.data_centers;
            if (dcInfo == null)
            {
                return new List<dynamic>(0);
            }
            return dcInfo
                   .Select(dc => dc.nodes)
                   .Where(nodes => nodes != null)
                   .SelectMany<dynamic, dynamic>(nodes => nodes)
                   .Where(n => n.queries != null)
                   .SelectMany<dynamic, dynamic>(n => n.queries)
                   .Where(q => (q.type == queryType || queryType == null) && (q.query == query || query == null))
                   .ToArray();
        }
    }
}