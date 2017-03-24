using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class SCassandraManager
    {
        private Process _scassandraProcess;
        public int BinaryPort { get; private set; }
        private readonly int _adminPort;

        private Uri BaseAddress
        {
            get { return new Uri("http://127.0.0.1:" + _adminPort); }
        }

        private static SCassandraManager _instance;

        public static SCassandraManager Instance
        {
            get
            {
                if (_instance != null) return _instance;
                _instance = new SCassandraManager();
                _instance.Start();
                _instance.SetupInitialConf().Wait();
                return _instance;
            }
        }

        public SCassandraManager(int binaryPort = 8042, int adminPort = 8043)
        {
            BinaryPort = binaryPort;
            _adminPort = adminPort;
        }

        public void Start()
        {
            Stop();
            _scassandraProcess = new Process();
            var jarPath = Environment.GetEnvironmentVariable("SCASSANDRA_JAR");
            if (string.IsNullOrEmpty(jarPath))
            {
                throw new Exception("SCassandra: SCASSANDRA_JAR environment variable not set!");
            }
            _scassandraProcess.StartInfo.FileName = "java";
            _scassandraProcess.StartInfo.Arguments = string.Format("-jar {0}  -Dscassandra.binary.port={1} -Dscassandra.binary.admin={2}", jarPath, BinaryPort, _adminPort);
            _scassandraProcess.StartInfo.UseShellExecute = false;
            _scassandraProcess.StartInfo.CreateNoWindow = true;
            _scassandraProcess.StartInfo.RedirectStandardOutput = true;
            _scassandraProcess.StartInfo.RedirectStandardError = true;
#if !NETCORE
            _scassandraProcess.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
#endif
            var eventWaitHandler = new AutoResetEvent(false);
            var primingPortReady = false;
            var scassandraPortReady = false;
            var isReady = false;
            var isPortAlreadyUsed = false;
            _scassandraProcess.OutputDataReceived += (sender, e) =>
            {
                if (e.Data == null || isReady) return;
                if (e.Data.Contains("Port " +  BinaryPort + " ready for Cassandra binary connections"))
                {
                    scassandraPortReady = true;
                }
                if (e.Data.Contains("Bound to localhost/127.0.0.1:" + _adminPort))
                {
                    primingPortReady = true;
                }

                if (e.Data.Contains("Unable to bind to port " + BinaryPort))
                {
                    isPortAlreadyUsed = true;
                    eventWaitHandler.Set();
                }

                if (scassandraPortReady && primingPortReady)
                {
                    isReady = true;
                    eventWaitHandler.Set();
                }
            };
            _scassandraProcess.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data == null) return;
                Console.WriteLine(e.Data);
            };
            _scassandraProcess.Start();

            _scassandraProcess.BeginOutputReadLine();
            _scassandraProcess.BeginErrorReadLine();

            eventWaitHandler.WaitOne(20000);
            if (isPortAlreadyUsed)
            {
                Stop();
                throw new Exception("Scassandra not started, port 8042 already in use");
            }
            if (!isReady)
            {
                Stop();
                throw new Exception("SCassandra not started!");
            }
        }

        public void Stop()
        {
            if (_scassandraProcess == null) return;

            try
            {
                _scassandraProcess.Kill();
                _scassandraProcess.Dispose();
            }
            catch
            {
                //ignore 
            }
            finally
            {
                _scassandraProcess = null;
            }
        }

        public async Task SetupInitialConf()
        {
            const string cql = "SELECT * FROM system.local WHERE key='local'";
            const string result = "{" +
                                  "\"rows\": " +
                                  "[" +
                                    "{" +
                                        "\"cluster_name\":\"custom cluster name\"," +
                                        "\"partitioner\":\"org.apache.cassandra.dht.Murmur3Partitioner\"," +
                                        "\"data_center\":\"dc1\"," +
                                        "\"rack\":\"rc1\"," +
                                        "\"tokens\":[\"1743244960790844724\"]," +
                                        "\"release_version\":\"2.0.1\"" +
                                    "}" +
                                  "]," +
                                  "\"result\":\"success\"," +
                                  "\"column_types\":{\"tokens\":\"set<text>\"}}";
            await PrimeQuery(cql, result);
        }

        public async Task<int[]> GetListOfConnectedPorts()
        {
            //current/connections
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.GetAsync("/current/connections");
                if (response.IsSuccessStatusCode)
                {
                    var connectionsStr = await response.Content.ReadAsStringAsync();
                    var connections = JsonConvert.DeserializeObject<ConnectionsResponse>(connectionsStr);
                    var ports = connections.connections.Select(x => x.port);
                    return ports.ToArray();
                }

            }
            return null;
        }

        public async Task DropConnection(int port)
        {
            //current/connections/127.0.0.1/<port>
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.DeleteAsync(string.Format("/current/connections/127.0.0.1/{0}", port));
                response.EnsureSuccessStatusCode();
            }
        }

        public async Task DisableConnectionListener()
        {
            //http://127.0.0.1:9043/current/listener
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.DeleteAsync("/current/listener");
                response.EnsureSuccessStatusCode();
            }
        }
        public async Task EnableConnectionListener()
        {
            //http://127.0.0.1:9043/current/listener
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PutAsync("/current/listener", null);
                response.EnsureSuccessStatusCode();
            }
        }

        public async Task PrimeQuery(string cql, string result)
        {
            //{
            //   "when": {
            //       "query" :"select * from people"
            //   },
            //   "then": {
            //       "result" : "read_request_timeout"
            //   }
            //}
            var bodyFormatJson = "{" +
                                    "\"when\" : {" +
                                        "\"query\" : \"" + cql + "\"" +
                                    "}," +
                                    "\"then\" : " + result +
                                 "}";
            var content = new StringContent(bodyFormatJson, Encoding.UTF8,
                                                            "application/json");

            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.PostAsync("/prime-query-single", content);
                response.EnsureSuccessStatusCode();
            }
        }

        public async Task ClearPrimedQueries()
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = BaseAddress;
                var response = await client.DeleteAsync("/prime-query-single");
                response.EnsureSuccessStatusCode();
            }
        }
    }

    class ConnectionsResponse
    {
        public ConnectionPort[] connections { get; set; }
    }

    class ConnectionPort
    {
        public string host { get; set; }
        public int port { get; set; }
    }
}
