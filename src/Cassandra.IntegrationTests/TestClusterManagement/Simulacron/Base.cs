using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class Base
    {
        public string Id { get; set; }

        protected Base(string id)
        {
            Id = id;
        }

        protected static async Task<JObject> Post(string url, JObject body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                bodyStr = body.ToString();
            }
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PostAsync(url, content);
                if (!response.IsSuccessStatusCode)
                {
                    return null;
                }
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task<JObject> Put(string url, JObject body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                bodyStr = body.ToString();
            }
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.PutAsync(url, content);
                response.EnsureSuccessStatusCode();
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task<JObject> Get(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.GetAsync(url);
                response.EnsureSuccessStatusCode();
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task Delete(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = SimulacronManager.BaseAddress;
                var response = await client.DeleteAsync(url);
                response.EnsureSuccessStatusCode();
            }
        }

        public JObject GetLogs()
        {
            return Get(GetPath("log")).Result;
        }

        public JObject Prime(JObject body)
        {
            return Post(GetPath("prime"), body).Result;
        }

        protected string GetPath(string endpoint)
        {
            return "/" + endpoint + "/" + Id;
        }

        public JObject GetConnections()
        {
            return Get(GetPath("connections")).Result;
        }

        public Task DisableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Delete(GetPath("listener") + "?after=" + attempts + "&type=" + type);
        }
    }
}