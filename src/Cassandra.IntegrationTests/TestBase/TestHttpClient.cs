//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;

using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.TestBase
{
    internal class TestHttpClient
    {
        private readonly Uri _baseAddress;

        public const string Post = "POST";
        public const string Put = "PUT";
        public const string Get = "GET";
        public const string Delete = "DELETE";

        public TestHttpClient(Uri baseAddress)
        {
            _baseAddress = baseAddress;
        }

        public Task<string> SendAsync(string method, string url)
        {
            return SendAsync(method, url, null);
        }

        public async Task<T> SendWithJsonAsync<T>(string method, string url, object body)
        {
            HttpContent content = null;
            if (body != null)
            {
                var bodyStr = GetJsonFromObject(body);
                content = new StringContent(bodyStr, Encoding.UTF8, "application/json");
            }

            var dataStr = await SendAsync(method, url, content).ConfigureAwait(false);
            return string.IsNullOrEmpty(dataStr) ? default(T) : JsonConvert.DeserializeObject<T>(dataStr);
        }

        public async Task<string> SendAsync(string method, string url, HttpContent content)
        {
            HttpMethod httpMethod;
            switch (method)
            {
                case TestHttpClient.Put:
                    httpMethod = HttpMethod.Put;
                    break;

                case TestHttpClient.Get:
                    httpMethod = HttpMethod.Get;
                    break;

                case TestHttpClient.Delete:
                    httpMethod = HttpMethod.Delete;
                    break;

                case TestHttpClient.Post:
                    httpMethod = HttpMethod.Post;
                    break;

                default:
                    throw new ArgumentException($"{method} not recognized.", nameof(method));
            }

            using (var client = new HttpClient())
            {
                client.BaseAddress = _baseAddress;
                var message = new HttpRequestMessage(httpMethod, url);
                if (content != null)
                {
                    message.Content = content;
                }
                var response = await client.SendAsync(message).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Invalid status code received {response.StatusCode}.{Environment.NewLine}" +
                                        $"{await response.Content.ReadAsStringAsync().ConfigureAwait(false)}");
                }

                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return dataStr;
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
    }
}