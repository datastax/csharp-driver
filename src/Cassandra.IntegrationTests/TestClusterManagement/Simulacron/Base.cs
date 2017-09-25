// //
// //      Copyright (C) 2017 DataStax Inc.
// //
// //   Licensed under the Apache License, Version 2.0 (the "License");
// //   you may not use this file except in compliance with the License.
// //   You may obtain a copy of the License at
// //
// //      http://www.apache.org/licenses/LICENSE-2.0
// //
// //   Unless required by applicable law or agreed to in writing, software
// //   distributed under the License is distributed on an "AS IS" BASIS,
// //   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //   See the License for the specific language governing permissions and
// //   limitations under the License.
// //

using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

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
            TestContext.WriteLine(bodyStr);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                Console.WriteLine(BaseAddress);
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
            Console.WriteLine(bodyStr);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                Console.WriteLine(BaseAddress);
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
                Console.WriteLine(BaseAddress);
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
                Console.WriteLine(BaseAddress);
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