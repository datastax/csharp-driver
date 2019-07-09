// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
                    args = " -REQUIRE_CLIENT_CERTIFICATE $True";
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

            ExecCommand(true, sniPath, args, envVars, fileInfo.Directory.FullName);
        }

        public void StopForce(int nodeIdToStop)
        {
            throw new System.NotImplementedException();
        }

        public void Stop(int nodeIdToStop)
        {
            throw new System.NotImplementedException();
        }

        public void Start(int nodeIdToStart, string additionalArgs = null)
        {
            throw new System.NotImplementedException();
        }

        public void Start(string[] jvmArgs = null)
        {
            throw new System.NotImplementedException();
        }

        public void UpdateConfig(params string[] yamlChanges)
        {
            throw new System.NotImplementedException();
        }

        public void InitClient()
        {
            throw new System.NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, bool start = true)
        {
            throw new System.NotImplementedException();
        }

        public void SetNodeWorkloads(int nodeId, string[] workloads)
        {
            throw new System.NotImplementedException();
        }

        public void BootstrapNode(int nodeIdToStart, string dataCenterName, bool start = true)
        {
            throw new System.NotImplementedException();
        }

        public void DecommissionNode(int nodeId)
        {
            throw new System.NotImplementedException();
        }

        public void DecommissionNodeForcefully(int nodeId)
        {
            throw new System.NotImplementedException();
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
            throw new System.NotImplementedException();
        }

        private static ProcessOutput ExecCommand(bool throwOnProcessError, string executable, string args, IReadOnlyDictionary<string, string> envVars = null, string workDir = null)
        {
            Trace.TraceInformation($"{executable} {args}");
            var output = CcmProcessExecuter.ExecuteProcess(executable, args, envVariables: envVars, workDir: workDir);

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
