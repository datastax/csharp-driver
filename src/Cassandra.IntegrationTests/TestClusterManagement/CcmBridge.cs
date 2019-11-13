//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmBridge : IDisposable
    {
        public DirectoryInfo CcmDir { get; private set; }
        public const int DefaultCmdTimeout = 90 * 1000;
        public const int StartCmdTimeout = 150 * 1000;
        public string Name { get; private set; }
        public string Version { get; private set; }
        public string IpPrefix { get; private set; }
        public ICcmProcessExecuter CcmProcessExecuter { get; set; }
        private readonly string _dseInstallPath;

        public CcmBridge(string name, string ipPrefix, string dsePath, string version, ICcmProcessExecuter executor)
        {
            Name = name;
            IpPrefix = ipPrefix;
            CcmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            CcmProcessExecuter = executor;
            _dseInstallPath = dsePath;
            Version = version;
        }

        public void Dispose()
        {
        }

        public void Create(bool useSsl)
        {
            var sslParams = "";
            if (useSsl)
            {
                var sslPath = Path.Combine(TestHelper.GetHomePath(), "ssl");
                if (!File.Exists(Path.Combine(sslPath, "keystore.jks")))
                {
                    throw new Exception(string.Format("In order to use SSL with CCM you must provide have the keystore.jks and cassandra.crt files located in your {0} folder", sslPath));
                }
                sslParams = "--ssl " + sslPath;
            }

            if (string.IsNullOrEmpty(_dseInstallPath))
            {
                ExecuteCcm(string.Format(
                    "create {0} --dse -v {1} {2}", Name, Version, sslParams));
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

        public void Start(string[] jvmArgs)
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
            if (jvmArgs != null)
            {
                foreach (var arg in jvmArgs)
                {
                    parameters.Add("--jvm_arg");
                    parameters.Add(arg);
                }
            }
            ExecuteCcm(string.Join(" ", parameters), StartCmdTimeout);
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
            ExecuteCcm(switchCmd, DefaultCmdTimeout, false);
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

        public void Start(int n, string additionalArgs = null)
        {
            string quietWindows = null;
            if (TestUtils.IsWin && CcmProcessExecuter is LocalCcmProcessExecuter)
            {
                quietWindows = "--quiet-windows";
            }
            ExecuteCcm(string.Format("node{0} start --wait-for-binary-proto {1} {2}", n, additionalArgs, quietWindows));
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

        public void BootstrapNode(int n, string dc, bool start = true)
        {
            ExecuteCcm(string.Format("add node{0} -i {1}{2} -j {3} -b -s {4} --dse", n, IpPrefix, n, 7000 + 100 * n, dc != null ? "-d " + dc : null));
            if (start)
            {
                Start(n);
            }
        }

        public void DecommissionNode(int n)
        {
            ExecuteCcm(string.Format("node{0} decommission", n));
        }

        public ProcessOutput ExecuteCcm(string args, int timeout = DefaultCmdTimeout, bool throwOnProcessError = true)
        {
            return CcmProcessExecuter.ExecuteCcm(args, timeout, throwOnProcessError);
        }

        public void UpdateConfig(params string[] configs)
        {
            if (configs == null)
            {
                return;
            }
            foreach (var c in configs)
            {
                ExecuteCcm(string.Format("updateconf \"{0}\"", c));
            }
        }

        public void UpdateDseConfig(params string[] configs)
        {
            if (configs == null)
            {
                return;
            }
            foreach (var c in configs)
            {
                ExecuteCcm(string.Format("updatedseconf \"{0}\"", c));
            }
        }

        public void SetNodeWorkloads(int nodeId, string[] workloads)
        {
            ExecuteCcm(string.Format("node{0} setworkload {1}", nodeId, string.Join(",", workloads)));
        }

        /// <summary>
        /// Sets the workloads for all nodes.
        /// </summary>
        public void SetWorkloads(int nodeLength, string[] workloads)
        {
            if (workloads == null || workloads.Length == 0)
            {
                return;
            }
            for (var nodeId = 1; nodeId <= nodeLength; nodeId++)
            {
                SetNodeWorkloads(nodeId, workloads);
            }
        }

        /// <summary>
        /// Spawns a new process (platform independent)
        /// </summary>
        public static ProcessOutput ExecuteProcess(
            string processName, 
            string args, 
            int timeout = DefaultCmdTimeout, 
            IReadOnlyDictionary<string, string> envVariables = null, 
            string workDir = null)
        {
            var output = new ProcessOutput();
            using (var process = new Process())
            {
                process.StartInfo.FileName = processName;
                process.StartInfo.Arguments = args;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                //Hide the python window if possible
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.CreateNoWindow = true;
#if !NETCORE
                process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
#endif
                
                if (envVariables != null)
                {
                    foreach (var envVar in envVariables)
                    {
                        process.StartInfo.EnvironmentVariables[envVar.Key] = envVar.Value;
                    }
                }

                if (workDir != null)
                {
                    process.StartInfo.WorkingDirectory = workDir;
                }

                using (var outputWaitHandle = new AutoResetEvent(false))
                using (var errorWaitHandle = new AutoResetEvent(false))
                {
                    process.OutputDataReceived += (sender, e) =>
                    {
                        if (e.Data == null)
                        {
                            try
                            {
                                outputWaitHandle.Set();
                            }
                            catch
                            {
                                //probably is already disposed
                            }
                        }
                        else
                        {
                            output.OutputText.AppendLine(e.Data);
                        }
                    };
                    process.ErrorDataReceived += (sender, e) =>
                    {
                        if (e.Data == null)
                        {
                            try
                            {
                                errorWaitHandle.Set();
                            }
                            catch
                            {
                                //probably is already disposed
                            }
                        }
                        else
                        {
                            output.OutputText.AppendLine(e.Data);
                        }
                    };
                    
                    try
                    {
                        process.Start();
                    }
                    catch (Exception exception)
                    {
                        Trace.TraceInformation("Process start failure: " + exception.Message);
                    }

                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    if (process.WaitForExit(timeout) &&
                        outputWaitHandle.WaitOne(timeout) &&
                        errorWaitHandle.WaitOne(timeout))
                    {
                        // Process completed.
                        output.ExitCode = process.ExitCode;
                    }
                    else
                    {
                        // Timed out.
                        output.ExitCode = -1;
                    }
                }
            }
            return output;
        }
    }
}
