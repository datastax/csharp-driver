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
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmBridge : IDisposable
    {
        public DirectoryInfo CcmDir { get; private set; }
        public string Name { get; private set; }
        public string Version { get; private set; }
        public string ScyllaVersion { get; private set; }
        public string IdPrefix { get; private set; }
        public string IpPrefix => $"127.0.{IdPrefix}.";
        public ICcmProcessExecuter CcmProcessExecuter { get; set; }
        private readonly string _dseInstallPath;

        public CcmBridge(string name, string idPrefix, string dsePath, string version, string scyllaVersion, ICcmProcessExecuter executor)
        {
            Name = name;
            IdPrefix = idPrefix;
            CcmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            CcmProcessExecuter = executor;
            _dseInstallPath = dsePath;
            Version = version;
            ScyllaVersion = scyllaVersion;
        }

        public void Dispose()
        {
        }

        public void Create(bool useSsl)
        {
            var sslParams = "";
            if (useSsl)
            {
                var sslPath = Environment.GetEnvironmentVariable("CCM_SSL_PATH");
                if (sslPath == null)
                {
                    sslPath = Path.Combine(TestHelper.GetHomePath(), "ssl");
                    if (!File.Exists(Path.Combine(sslPath, "keystore.jks")))
                    {
                        throw new Exception(string.Format("In order to use SSL with CCM you must provide have the keystore.jks and cassandra.crt files located in your {0} folder", sslPath));
                    }
                }
                sslParams = "--ssl " + sslPath;
            }

            if (!string.IsNullOrEmpty(ScyllaVersion))
            {
                ExecuteCcm($"create {Name} --scylla -v release:{ScyllaVersion} {sslParams}");
            }
            else if (string.IsNullOrEmpty(_dseInstallPath))
            {
                if (TestClusterManager.IsDse)
                {
                    ExecuteCcm(string.Format(
                        "create {0} --dse -v {1} {2}", Name, Version, sslParams));
                }
                else if(TestClusterManager.CurrentBackendType == TestClusterManager.BackendType.Hcd)
                {
                    ExecuteCcm(string.Format(
                        "create {0} --hcd -v {1} {2}", Name, Version, sslParams));
                }
                else
                {
                    ExecuteCcm(string.Format(
                        "create {0} -v {1} {2}", Name, Version, sslParams));
                }
            }
            else
            {
                ExecuteCcm(string.Format(
                    "create {0} --install-dir={1} {2}", Name, _dseInstallPath, sslParams));
            }
        }

        protected string GetHomePath()
        {
            var home = Environment.GetEnvironmentVariable("USERPROFILE");
            if (!string.IsNullOrEmpty(home))
            {
                return home;
            }
            home = Environment.GetEnvironmentVariable("HOME");
            if (string.IsNullOrEmpty(home))
            {
                throw new NotSupportedException("HOME or USERPROFILE are not defined");
            }
            return home;
        }

        public ProcessOutput Start(string[] jvmArgs)
        {
            var parameters = new List<string>
            {
                "start",
                "--wait-for-binary-proto"
            };
            if (TestUtils.IsWin && CcmProcessExecuter is LocalCcmProcessExecuter)
            {
                parameters.Add("--quiet-windows");
            }
            if (CcmProcessExecuter is WslCcmProcessExecuter)
            {
                parameters.Add("--root");
            }
            if (jvmArgs != null)
            {
                foreach (var arg in jvmArgs)
                {
                    parameters.Add("--jvm_arg");
                    parameters.Add(arg);
                }
            }

            if (CcmProcessExecuter is WslCcmProcessExecuter)
            {
                return ExecuteCcm(string.Join(" ", parameters), false);
            }
            else
            {
                return ExecuteCcm(string.Join(" ", parameters));
            }
        }

        public ProcessOutput Start(int n, string additionalArgs = null, string[] jvmArgs = null)
        {
            string quietWindows = null;
            string runAsRoot = null;
            if (TestUtils.IsWin && CcmProcessExecuter is LocalCcmProcessExecuter)
            {
                quietWindows = "--quiet-windows";
            }

            if (CcmProcessExecuter is WslCcmProcessExecuter)
            {
                runAsRoot = "--root";
            }

            var jvmArgsParameters = new List<string>
            {
                "start",
                "--wait-for-binary-proto"
            };
            if (jvmArgs != null)
            {
                foreach (var arg in jvmArgs)
                {
                    jvmArgsParameters.Add("--jvm_arg");
                    jvmArgsParameters.Add(arg);
                }
            }

            var jvmArgsStr = string.Join(" ", jvmArgsParameters);

            if (CcmProcessExecuter is WslCcmProcessExecuter)
            {
                return ExecuteCcm($"node{n} start --wait-for-binary-proto {additionalArgs} {quietWindows} {runAsRoot} {jvmArgsStr}", false);
            }
            else
            {
                return ExecuteCcm($"node{n} start --wait-for-binary-proto {additionalArgs} {quietWindows} {runAsRoot} {jvmArgsStr}");
            }
        }

        public void CheckNativePortOpen(ProcessOutput output, string ip)
        {
            using (var ccmConnection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                using (var cts = new CancellationTokenSource(5 * 60 * 1000))
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            ccmConnection.Connect(ip, 9042);
                            return;
                        }
                        catch
                        {
                            Thread.Sleep(5000);
                        }
                    }
                }
            }

            throw new TestInfrastructureException("Native Port check timed out. Output: " + Environment.NewLine + output.ToString());
        }

        public void Populate(int dc1NodeLength, int dc2NodeLength, bool useVNodes)
        {
            var parameters = new List<string>
            {
                "populate",
                "-n",
                dc1NodeLength + (dc2NodeLength > 0 ? ":" + dc2NodeLength : null),
                "-i",
                IpPrefix
            };
            if (useVNodes)
            {
                parameters.Add("--vnodes");
            }
            ExecuteCcm(string.Join(" ", parameters));
        }

        public void SwitchToThis()
        {
            string switchCmd = "switch " + Name;
            ExecuteCcm(switchCmd, false);
        }

        public void List()
        {
            ExecuteCcm("list");
        }

        public void Stop()
        {
            ExecuteCcm("stop");
        }

        public void StopForce()
        {
            ExecuteCcm("stop --not-gently");
        }

        public void Stop(int n)
        {
            ExecuteCcm(string.Format("node{0} stop", n));
        }

        public void StopForce(int n)
        {
            ExecuteCcm(string.Format("node{0} stop --not-gently", n));
        }

        public void Remove()
        {
            ExecuteCcm("remove");
        }

        public void Remove(int nodeId)
        {
            ExecuteCcm(string.Format("node{0} remove", nodeId));
        }

        public void BootstrapNode(int n, bool start = true)
        {
            BootstrapNode(n, null, start);
        }

        public ProcessOutput BootstrapNode(int n, string dc, bool start = true)
        {
            var cmd = "add node{0} -i {1}{2} -j {3} -b -s {4}";
            if (TestClusterManager.IsDse)
            {
                cmd += " --dse";
            }
            else if (TestClusterManager.CurrentBackendType == TestClusterManager.BackendType.Hcd)
            {
                cmd += " --hcd";
            }
            else if (TestClusterManager.IsScylla)
            {
               cmd += " --scylla";
            }

            var output = ExecuteCcm(string.Format(cmd, n, IpPrefix, n, 7000 + 100 * n, dc != null ? "-d " + dc : null));

            if (start)
            {
                return Start(n);
            }

            return output;
        }

        public void DecommissionNode(int n)
        {
            ExecuteCcm(string.Format("node{0} decommission", n));
        }

        public ProcessOutput ExecuteCcm(string args, bool throwOnProcessError = true)
        {
            return CcmProcessExecuter.ExecuteCcm(args, throwOnProcessError);
        }

        public void UpdateConfig(params string[] configs)
        {
            if (configs == null)
            {
                return;
            }
            FixYaml(configs);
            var joinedConfigs = string.Join(" ", configs.Select(s => $"\"{s}\""));
            ExecuteCcm($"updateconf {joinedConfigs}");
        }

        public void UpdateDseConfig(params string[] configs)
        {
            if (!TestClusterManager.IsDse)
            {
                throw new InvalidOperationException("Cant update dse config on an oss cluster.");
            }

            if (configs == null)
            {
                return;
            }
            FixYaml(configs);
            var joinedConfigs = string.Join(" ", configs.Select(s => $"\"{s}\""));
            ExecuteCcm($"updatedseconf {joinedConfigs}");
        }

        public void UpdateConfig(int nodeId, params string[] yamlChanges)
        {
            if (yamlChanges == null) return;
            FixYaml(yamlChanges);
            var joinedChanges = string.Join(" ", yamlChanges.Select(s => $"\"{s}\""));
            ExecuteCcm($"node{nodeId} updateconf {joinedChanges}");
        }

        private static void FixYaml(string[] yamlToFix)
        {
            // in-place fix
            if (TestClusterManager.CheckCassandraVersion(false, System.Version.Parse("4.1"), Comparison.GreaterThanOrEqualsTo))
            {
                // Fix the yaml options that turned obsolete since 4.1.0
                for (int i = 0; i < yamlToFix.Length; i++)
                {
                    string line = yamlToFix[i];
                    var keyValueParts = line.Split(':');

                    var key = keyValueParts[0];
                    var value = keyValueParts[1];

                    var matchMs = Regex.Match(key, @"^(\w+)_in_ms$");
                    if (matchMs.Success)
                    {
                        yamlToFix[i] = $"{matchMs.Groups[1].Value}:{value}ms";
                    }

                    var matchKb = Regex.Match(key, @"^(\w+)_in_kb$");
                    if (matchKb.Success)
                    {
                        yamlToFix[i] = $"{matchKb.Groups[1].Value}:{value}KiB";
                    }

                    var matchEnable = Regex.Match(key, @"enable_(\w+)$");
                    if (matchEnable.Success)
                    {
                        yamlToFix[i] = $"{matchEnable.Groups[1].Value}_enabled:{value}";
                    }
                }
            }
        }


        public void SetNodeWorkloads(int nodeId, string[] workloads)
        {
            if (!TestClusterManager.IsDse)
            {
                throw new InvalidOperationException("Cant set workloads on an oss cluster.");
            }

            ExecuteCcm(string.Format("node{0} setworkload {1}", nodeId, string.Join(",", workloads)));
        }

        /// <summary>
        /// Sets the workloads for all nodes.
        /// </summary>
        public void SetWorkloads(int nodeLength, string[] workloads)
        {
            if (!TestClusterManager.IsDse)
            {
                throw new InvalidOperationException("Cant set workloads on an oss cluster.");
            }

            if (workloads == null || workloads.Length == 0)
            {
                return;
            }
            for (var nodeId = 1; nodeId <= nodeLength; nodeId++)
            {
                SetNodeWorkloads(nodeId, workloads);
            }
        }
    }
}
