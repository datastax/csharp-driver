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
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class SimulacronManager
    {
        private const string CreateClusterPathFormat = "/cluster?data_centers={0}&cassandra_version={1}&dse_version={2}&name={3}" +
                                                       "&activity_log={4}&num_tokens={5}";
        private const string CreateClusterPath = "/cluster";

        private volatile Process _simulacronProcess;

        private volatile bool _initialized;

        private static volatile SimulacronManager _currentInstance = null;

        private static readonly object GlobalLock = new object();

        public static SimulacronManager DefaultInstance { get; } = new SimulacronManager();
        
        public static SimulacronManager InstancePeersV2Tests { get; } = new SimulacronManager(9011);
        
        public Uri BaseAddress => new Uri($"http://127.0.0.1:{HttpPort}");

        public int? StartPort { get; } = null;

        public string StartIp { get; } = "127.0.0.101";

        public int HttpPort { get; } = 8188;

        private SimulacronManager()
        {
        }

        private SimulacronManager(int? startPort)
        {
            StartPort = startPort;
        }

        public static SimulacronManager GetForPeersTests()
        {
            return SimulacronManager.InstancePeersV2Tests;
        }

        public void Start()
        {
            if (_initialized)
            {
                return;
            }

            lock (SimulacronManager.GlobalLock)
            {
                if (_initialized)
                {
                    return;
                }

                if (SimulacronManager._currentInstance != null)
                {
                    SimulacronManager._currentInstance.Stop();
                }

                SimulacronManager._currentInstance = this;

                var started = false;
                var errorMessage = "Simulacron is taking too long to start. Aborting initialization...";
                _simulacronProcess = new Process();
                var jarPath = Environment.GetEnvironmentVariable("SIMULACRON_PATH");
                if (string.IsNullOrEmpty(jarPath))
                {
                    jarPath = Environment.GetEnvironmentVariable("HOME") + "/simulacron.jar";
                }
                if (!File.Exists(jarPath))
                {
                    throw new Exception("Simulacron: Simulacron jar not found: " + jarPath);
                }

                var args = $"-jar {jarPath} --ip {StartIp} -p {HttpPort}";
                if (StartPort.HasValue)
                {
                    args += $" -s {StartPort}";
                }

                _simulacronProcess.StartInfo.FileName = "java";
                _simulacronProcess.StartInfo.Arguments = args;
                _simulacronProcess.StartInfo.UseShellExecute = false;
                _simulacronProcess.StartInfo.CreateNoWindow = true;
                _simulacronProcess.StartInfo.RedirectStandardOutput = true;
                _simulacronProcess.StartInfo.RedirectStandardError = true;
                _simulacronProcess.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

                var eventWaitHandler = new AutoResetEvent(false);
                _simulacronProcess.OutputDataReceived += (sender, e) =>
                {
                    if (e.Data == null || started) return;
                    Trace.TraceInformation(e.Data);
                    if (e.Data.Contains("Address already in use"))
                    {
                        errorMessage = $"Simulacron start error: {e.Data}";
                        eventWaitHandler.Set();
                        return;
                    }
                    if (e.Data.Contains("Created nodes will start with ip"))
                    {
                        started = true;
                        eventWaitHandler.Set();
                    }
                };
                _simulacronProcess.ErrorDataReceived += (sender, e) =>
                {
                    if (e.Data == null) return;
                    Trace.TraceError(e.Data);
                    errorMessage = $"Simulacron start error: {e.Data}";
                };
                _simulacronProcess.Start();

                _simulacronProcess.BeginOutputReadLine();
                _simulacronProcess.BeginErrorReadLine();

                eventWaitHandler.WaitOne(30000);
                if (!started)
                {
                    Trace.TraceError(errorMessage);
                    Stop();
                    SimulacronManager._currentInstance = null;
                    throw new Exception("Simulacron failed to start! " + Environment.NewLine + errorMessage);
                }
                _initialized = true;
                Trace.TraceInformation("Simulacron started");
            }
        }

        public void Stop()
        {
            _initialized = false;
            if (_simulacronProcess == null) return;

            try
            {
                _simulacronProcess.Kill();

                if (!_simulacronProcess.WaitForExit(30000))
                {
                    throw new TimeoutException("Simulacron process didn't stop after kill signal.");
                }

                _simulacronProcess.Dispose();
            }
            catch (Exception ex)
            {
                Trace.TraceWarning(ex.Message);
            }
            finally
            {
                _simulacronProcess = null;
                Trace.TraceInformation("Simulacron process stopped");
            }
        }
        
        public Task<SimulacronCluster> CreateNewAsync(int nodeLength)
        {
            return CreateNewAsync(new SimulacronOptions { Nodes = nodeLength.ToString() });
        }

        /// <summary>
        /// Creates a single DC cluster with the amount of nodes provided.
        /// </summary>
        public SimulacronCluster CreateNew(int nodeLength)
        {
            return CreateNew(new SimulacronOptions { Nodes = nodeLength.ToString() });
        }
        
        public async Task<SimulacronCluster> CreateNewAsync(SimulacronOptions options)
        {
            Start();
            var path = string.Format(CreateClusterPathFormat, options.Nodes, options.GetCassandraVersion(),
                options.GetDseVersion(), options.Name, options.ActivityLog, options.NumberOfTokens);
            var data = await Post(path, null).ConfigureAwait(false);
            return CreateFromData(data);
        }

        public SimulacronCluster CreateNew(SimulacronOptions options)
        {
            return TaskHelper.WaitToComplete(CreateNewAsync(options));
        }
        
        /// <summary>
        /// Creates a new cluster with POST body parameters.
        /// </summary>
        public SimulacronCluster CreateNewWithPostBody(dynamic body)
        {
            Start();
            var data = TaskHelper.WaitToComplete(Post(CreateClusterPath, body));
            return CreateFromData(data);
        }

        private SimulacronCluster CreateFromData(dynamic data)
        {
            var cluster = new SimulacronCluster(data["id"].ToString(), this)
            {
                Data = data,
                DataCenters = new List<SimulacronDataCenter>()
            };
            var dcs = (JArray) cluster.Data["data_centers"];
            foreach (var dc in dcs)
            {
                var dataCenter = new SimulacronDataCenter(cluster.Id + "/" + dc["id"], this);
                cluster.DataCenters.Add(dataCenter);
                dataCenter.Nodes = new List<SimulacronNode>();
                var nodes = (JArray) dc["nodes"];
                foreach (var nodeJObject in nodes)
                {
                    var node = new SimulacronNode(dataCenter.Id + "/" + nodeJObject["id"], this);
                    dataCenter.Nodes.Add(node);
                    node.ContactPoint = nodeJObject["address"].ToString();
                }
            }
            return cluster;
        }
        
        public async Task<JObject> Post(string url, object body)
        {
            if (!_initialized)
            {
                throw new ObjectDisposedException("Simulacron Process not started.");
            }

            var bodyStr = GetJsonFromObject(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PostAsync(url, content).ConfigureAwait(false);
                await EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JObject.Parse(dataStr);
            }
        }

        public async Task<JObject> PutAsync(string url, object body)
        {
            if (!_initialized)
            {
                throw new ObjectDisposedException("Simulacron Process not started.");
            }

            var bodyStr = GetJsonFromObject(body);
            var content = new StringContent(bodyStr, Encoding.UTF8, "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PutAsync(url, content).ConfigureAwait(false);
                await EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                if (string.IsNullOrEmpty(dataStr))
                {
                    return null;
                }
                return JObject.Parse(dataStr);
            }
        }

        public async Task<T> GetAsync<T>(string url)
        {
            if (!_initialized)
            {
                throw new ObjectDisposedException("Simulacron Process not started.");
            }

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.GetAsync(url).ConfigureAwait(false);
                await EnsureSuccessStatusCode(response).ConfigureAwait(false);
                var dataStr = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                return JsonConvert.DeserializeObject<T>(dataStr);
            }
        }

        public async Task DeleteAsync(string url)
        {
            if (!_initialized)
            {
                throw new ObjectDisposedException("Simulacron Process not started.");
            }

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.DeleteAsync(url).ConfigureAwait(false);
                await EnsureSuccessStatusCode(response).ConfigureAwait(false);
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
