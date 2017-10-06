using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class SimulacronBase
    {
        public string Id { get; set; }

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
                var response = await client.PostAsync(url, content);
                if (!response.IsSuccessStatusCode)
                {
                    return null;
                }
                var dataStr = await response.Content.ReadAsStringAsync();
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
                var response = await client.PutAsync(url, content);
                response.EnsureSuccessStatusCode();
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected static async Task<dynamic> Get(string url)
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

        public dynamic GetLogs()
        {
            return Get(GetPath("log")).Result;
        }

        public dynamic Prime(dynamic body)
        {
            return Post(GetPath("prime"), body).Result;
        }

        protected string GetPath(string endpoint)
        {
            return "/" + endpoint + "/" + Id;
        }

        public dynamic GetConnections()
        {
            return Get(GetPath("connections")).Result;
        }

        public Task DisableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Delete(GetPath("listener") + "?after=" + attempts + "&type=" + type);
        }

        public Task<dynamic> EnableConnectionListener(int attempts = 0, string type = "unbind")
        {
            return Put(GetPath("listener") + "?after=" + attempts + "&type=" + type, null);
        }

    }
}