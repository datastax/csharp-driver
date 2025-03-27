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

#if NETCOREAPP
using System.Net.Http;
#endif

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
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
#if NETCOREAPP
            return SendAsync(method, url, null);
#endif

#if NETFRAMEWORK
            return SendAsync(method, url, null, null);
#endif
        }

#if NETCOREAPP
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
#endif

#if NETFRAMEWORK

        public async Task<T> SendWithJsonAsync<T>(string method, string url, object body)
        {
            byte[] content = null;
            if (body != null)
            {
                var bodyStr = GetJsonFromObject(body);
                content = Encoding.UTF8.GetBytes(bodyStr);
            }

            var data = await SendAsync(method, url, "application/json", content).ConfigureAwait(false);
            return string.IsNullOrEmpty(data)
                ? default(T)
                : JsonConvert.DeserializeObject<T>(data);
        }

        public async Task<string> SendAsync(string method, string url, string contentType, byte[] content)
        {
            var request = (HttpWebRequest)WebRequest.Create(_baseAddress + "/" + url);

            request.KeepAlive = false;
            request.Method = method;

            if (content != null)
            {
                request.ContentType = contentType;
                request.ContentLength = content.Length;
                using (var dataStream = request.GetRequestStream())
                {
                    dataStream.Write(content, 0, content.Length);
                    dataStream.Close();
                }
            }

            using (var response = (HttpWebResponse)request.GetResponse())
            {
                using (var dataStream = response.GetResponseStream())
                {
                    var reader = new StreamReader(dataStream);
                    var responseFromServer = await reader.ReadToEndAsync().ConfigureAwait(false);
                    var statusCode = (int)response.StatusCode;
                    if (statusCode < 200 || statusCode >= 300)
                    {
                        throw new Exception($"Invalid status code received {statusCode}.{Environment.NewLine}" +
                                            $"{responseFromServer}");
                    }

                    return responseFromServer;
                }
            }
        }

#endif

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