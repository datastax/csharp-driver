using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement.Simulacron
{
    public class Base
    {
        public Uri BaseAddress { get; set; }
        public string Id { get; set; }

        protected async Task<JObject> Post(string url, JObject body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                bodyStr = body.ToString();
            }
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PostAsync(url, content);
                if (!response.IsSuccessStatusCode)
                {
                    return null;
                }
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected async Task<JObject> Put(string url, JObject body)
        {
            var bodyStr = string.Empty;
            if (body != null)
            {
                bodyStr = body.ToString();
            }
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PutAsync(url, content);
                if (!response.IsSuccessStatusCode) return null;
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected async Task<JObject> Get(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.GetAsync(url);
                response.EnsureSuccessStatusCode();
                if (!response.IsSuccessStatusCode) return null;
                var dataStr = await response.Content.ReadAsStringAsync();
                return JObject.Parse(dataStr);
            }
        }

        protected async Task Delete(string url)
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
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
    }
}