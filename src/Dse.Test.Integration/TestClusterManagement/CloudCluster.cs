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
using Dse.Test.Unit;

namespace Dse.Test.Integration.TestClusterManagement
{
    public class CloudCluster : ITestCluster
    {
        public string Name { get; set; }

        public string Version { get; set; }

        public Builder Builder { get; set; }

        public Cluster Cluster { get; set; }

        public ISession Session { get; set; }

        public string InitialContactPoint { get; set; }

        public string ClusterIpPrefix { get; set; }

        public string DefaultKeyspace { get; set; }

        public bool SniCertificateValidation { get; }

        public string SniHomeDirectory { get; private set; }

        public CloudCluster(string name, string version, bool enableCert)
        {
            SniCertificateValidation = enableCert;
            Name = name;
            InitialContactPoint = ClusterIpPrefix + "1";
            Version = version;
        }

        public void ShutDown()
        {
            DockerKill();
        }

        public void Remove()
        {
            throw new System.NotImplementedException();
        }

        public void Remove(int nodeId)
        {
            throw new System.NotImplementedException();
        }

        public void Create(int nodeLength, TestClusterOptions options = null)
        {
            var sniPath = Environment.GetEnvironmentVariable("SINGLE_ENDPOINT_PATH");
            if (sniPath == null)
            {
                sniPath = Path.Combine(Environment.GetEnvironmentVariable("HOME"), "proxy", "run.sh");
                Trace.TraceInformation("SINGLE_ENDPOINT_PATH not set, using " + sniPath);
            }

            var fileInfo = new FileInfo(sniPath);
            var args = string.Empty;
            var envVars = new Dictionary<string, string>();
            if (SniCertificateValidation && TestCloudClusterManager.CertFile != null)
            {
                if (sniPath.EndsWith("run.ps1"))
                {
                    args = " -REQUIRE_CLIENT_CERTIFICATE true";
                }
                else
                {
                    envVars["REQUIRE_CLIENT_CERTIFICATE"] = "true";
                }
            }

            if (SniCertificateValidation && TestCloudClusterManager.CertFile == null)
            {
                throw new InvalidOperationException("_enableCert is true but cert file env variable is false");
            }

            if (sniPath.EndsWith(".ps1"))
            {
                var oldSniPath = sniPath;
                sniPath = @"powershell";
                args = "\"& '" + oldSniPath + "'" + args + "\"";
            }
            
            if (envVars.ContainsKey("REQUIRE_CLIENT_CERTIFICATE")) 
            {
                args = @"-c ""export REQUIRE_CLIENT_CERTIFICATE=true && " + sniPath + "\"";
                sniPath = "bash";
            }

            SniHomeDirectory = fileInfo.Directory.FullName;

            ExecCommand(true, sniPath, args, envVars, fileInfo.Directory.FullName);
        }

        public void StopForce(int nodeIdToStop)
        {
            throw new System.NotImplementedException();
        }

        public void Stop(int nodeIdToStop)
        {
            ExecCcmCommand($"node{nodeIdToStop} stop");
        }

        public void Start(int nodeIdToStart, string additionalArgs = null)
        {
            ExecCcmCommand($"node{nodeIdToStart} start --root --wait-for-binary-proto {additionalArgs}");
        }

        public void Start(string[] jvmArgs = null)
        {
            ExecCcmCommand($"start --root --wait-for-binary-proto {jvmArgs}");
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            throw new System.NotImplementedException();
        }

        public void UpdateConfig(int nodeId, params string[] yamlChanges)
        {
            throw new NotImplementedException();
        }

        public void InitClient()
        {
            throw new System.NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, bool start = true)
        {
            throw new NotImplementedException();
        }

        public void SetNodeWorkloads(int nodeId, string[] workloads)
        {
            throw new NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName, bool start = true)
        {
            throw new NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart)
        {
            throw new NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName)
        {
            throw new NotImplementedException();
        }

        public void DecommissionNode(int nodeId)
        {
            throw new System.NotImplementedException();
        }

        public void DecommissionNodeForcefully(int nodeId)
        {
            throw new NotImplementedException();
        }

        public void PauseNode(int nodeId)
        {
            throw new System.NotImplementedException();
        }

        public void ResumeNode(int nodeId)
        {
            throw new System.NotImplementedException();
        }

        public void SwitchToThisCluster()
        {
            throw new NotImplementedException();
        }

        private static void ExecCcmCommand(string ccmCmd)
        {
            if (TestHelper.IsWin)
            {
                ccmCmd = ccmCmd.Replace("\"", "\"\"\"");
                ExecCommand(true, "powershell", $"-command \"docker ps -a -q --filter ancestor=single_endpoint | % {{ docker exec $_ ccm {ccmCmd} }}\"");
            }
            else
            {
                ccmCmd = ccmCmd.Replace("\"", @"\""");
                ExecCommand(true, "bash", $@"-c ""docker exec $(docker ps -a -q --filter ancestor=single_endpoint) ccm {ccmCmd}""");
            }
        }

        private static ProcessOutput ExecCommand(bool throwOnProcessError, string executable, string args, IReadOnlyDictionary<string, string> envVars = null, string workDir = null)
        {
            Trace.TraceInformation($"{executable} {args}");
            var output = CcmBridge.ExecuteProcess(executable, args, timeout: 300000, envVariables: envVars, workDir: workDir);

            if (!throwOnProcessError)
            {
                return output;
            }

            if (output.ExitCode != 0)
            {
                throw new TestInfrastructureException($"Process exited in error {output}");
            }

            return output;
        }

        public static void DockerKill()
        {
            if (TestHelper.IsWin)
            {
                ExecCommand(true, "powershell", "-command \"docker ps -a -q --filter ancestor=single_endpoint | % { docker kill $_ }\"");
            }
            else
            {
                ExecCommand(true, "bash", @"-c ""docker kill $(docker ps -a -q --filter ancestor=single_endpoint)""");
            }
        }
    }
}
