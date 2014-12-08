//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public class CcmBridge : TestGlobals, IDisposable
    {
        public DirectoryInfo CcmDir;
        private Renci.SshNet.SshClient _sshClient;
        private Renci.SshNet.ShellStream _sshShellStream;
        public const int DefaultCmdTimeout = 90 * 1000; 
        public string Name = null;
        public string IpPrefix = null;

        public CcmBridge(string name, string ipPrefix, bool instantiateSshClient = false)
        {
            Name = name;
            IpPrefix = ipPrefix;
            CcmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            if (instantiateSshClient)
            {
                _sshClient = new Renci.SshNet.SshClient(SSHHost, SSHPort, SSHUser, SSHPassword);
                _sshClient.Connect();

                _sshShellStream = _sshClient.CreateShellStream("CCM", 80, 60, 100, 100, 1000);
                var outp = new StringBuilder();
                while (true)
                {
                    outp.Append(_sshShellStream.Read());
                    if (outp.ToString().Trim().EndsWith("$"))
                        break;
                }
            }
        }

        public void Dispose()
        {
            if (_sshClient != null)
            {
                _sshClient.Disconnect();
                _sshClient = null;
            }
        }

        ~CcmBridge()
        {
            if (_sshClient != null)
                _sshClient.Disconnect();
        }

        public static CcmBridge Create(string name, string localIpPrefix, int nodeCount, string cassandraVersion, bool startTheCluster = true)
        {
            CcmBridge ccmBridge = new CcmBridge(name, localIpPrefix);
            string clusterStartStr = "";
            if (startTheCluster)
                clusterStartStr = "-s";
            ProcessOutput processOutput = ccmBridge.ExecuteCcm(string.Format("Create {0} -n {1} {2} -i {3} -b -v {4}", name, nodeCount, clusterStartStr, localIpPrefix, cassandraVersion));
            return ccmBridge;
        }

        public static CcmBridge Create(string name, string localIpPrefix, int nodeCountDc1, int nodeCountDc2, string cassandraVersion, bool startTheCluster = true, bool throwOnError = false)
        {
            CcmBridge ccmBridge = new CcmBridge(name, localIpPrefix);
            string clusterStartStr = "";
            if (startTheCluster)
                clusterStartStr = "-s";
            ccmBridge.ExecuteCcm(string.Format("Create {0} -n {1}:{2} {3} -i {4} -b -v {5}", name, nodeCountDc1, nodeCountDc2, clusterStartStr, localIpPrefix, cassandraVersion), DefaultCmdTimeout, throwOnError);
            return ccmBridge;
        }

        public void Start()
        {
            ExecuteCcm("start", DefaultCmdTimeout, false);
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

        public void Start(int n)
        {
            ExecuteCcm(string.Format("node{0} start", n));
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
            Stop();
            ExecuteCcm(string.Format("remove"));
        }

        public void Ring(int n)
        {
            ExecuteCCMAndPrint(string.Format("node{0} ring", n));
        }

        public void BootstrapNode(int n)
        {
            BootstrapNode(n, null);
        }

        public void BootstrapNode(int n, string dc)
        {
            if (dc == null)
                ExecuteCcm(string.Format("add node{0} -i {1}{2} -j {3} -b -s", n, IpPrefix, n, 7000 + 100 * n));
            else
                ExecuteCcm(string.Format("add node{0} -i {1}{2} -j {3} -b -s -d {4}", n, IpPrefix, n, 7000 + 100 * n, dc));
            ExecuteCcm(string.Format("node{0} start", n));
        }

        public void DecommissionNode(int n)
        {
            ExecuteCcm(string.Format("node{0} decommission", n));
        }

        private int dead = 0;

        public ProcessOutput ExecuteCcm(string args, int timeout = DefaultCmdTimeout, bool throwOnProcessError = false)
        {
            // right now having a config dir is always a requirement
            var ccmArgs = args + " --config-dir=" + CcmDir.FullName;
            Trace.TraceInformation("Executing cmd line: " + ccmArgs);
            var executable = "/usr/local/bin/ccm";
            if (TestUtils.IsWin)
            {
                executable = "cmd.exe";
                args = "/c ccm " + args;
            }
            ProcessOutput output = ExecuteProcess(executable, args, timeout);
            if (throwOnProcessError)
            {
                ValidateOutput(output);
            }
            return output;
        }

        private static void ValidateOutput(ProcessOutput output)
        {
            if (output.ExitCode != 0)
            {
                throw new TestInfrastructureException(string.Format("Process exited in error {0}", output.ToString()));
            }
        }

        /// <summary>
        /// Spawns a new process (platform independent)
        /// </summary>
        public static ProcessOutput ExecuteProcess(string processName, string args, int timeout = DefaultCmdTimeout)
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
                process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                process.StartInfo.CreateNoWindow = true;

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

                    process.Start();

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


        private void ExecuteCCMAndPrint(string args)
        {
            Trace.TraceInformation("CCM>"+args);
            _sshShellStream.WriteLine("ccm " + args /*+ " --config-dir=" + _ccmDir*/);
            var outp = new StringBuilder();
            while (true)
            {
                var txt = _sshShellStream.Read();
                outp.Append(txt);
                if (txt.Contains("$"))
                    break;
            }
            var iserror = outp.ToString().Contains("[Errno");
            var lines = outp.ToString().Split('\n');

            for (int i = 0; i < lines.Length - 1; i++)
            {
                if (iserror)
                    Trace.TraceError("err>" + lines[i].Trim());
                else
                    Trace.TraceInformation("out>" + lines[i].Trim());
            }

            if(iserror)
                throw new InvalidOperationException();

            Thread.Sleep(2000);
        }

        private void PureExecute(string args)
        {
            Trace.TraceInformation("SHELL>" + args);
            _sshShellStream.WriteLine(args);
            var outp = new StringBuilder();
            while (true)
            {
                var txt = _sshShellStream.Read();
                outp.Append(txt);
                if (txt.Contains("$"))
                    break;
            }
        }

        /// <summary>
        /// This class will go away soon.
        /// </summary>
        public static class ReusableCCMCluster
        {
            static int NbNodesDC1;
            static int NbNodesDC2;
            public static CcmBridge CCMBridge;

            internal static void Reset()
            {
                NbNodesDC1 = 0;
                NbNodesDC2 = 0;
            }

            public static void Setup(int nbNodesDC1, int nbNodesDC2, string ipPrefix, string cassandraVersion, string clusterName = "test")
            {
                if (nbNodesDC2 == 0)
                {
                    if (nbNodesDC1 != NbNodesDC1)
                    {
                        Trace.TraceInformation("Cassandra:" + cassandraVersion);
                        CCMBridge = CcmBridge.Create(clusterName, ipPrefix, nbNodesDC1, cassandraVersion);
                        NbNodesDC1 = nbNodesDC1;
                        NbNodesDC2 = 0;
                    }
                }
                else
                {
                    if (nbNodesDC1 != NbNodesDC1 || nbNodesDC2 != NbNodesDC2)
                    {
                        CCMBridge = CcmBridge.Create(clusterName, ipPrefix, nbNodesDC1, nbNodesDC2, cassandraVersion);
                        NbNodesDC1 = nbNodesDC1;
                        NbNodesDC2 = nbNodesDC2;
                    }
                }
            }

            static Cluster Cluster;
            static ISession Session;

            public static Cluster Build(Builder builder)
            {
                if (Options.Default.USE_COMPRESSION)
                {
                    builder.WithCompression(CompressionType.Snappy);
                    Trace.TraceInformation("Using Compression");
                }
                if (Options.Default.USE_NOBUFFERING)
                {
                    builder.WithoutRowSetBuffering();
                    Trace.TraceInformation("No buffering");
                }

                Cluster = builder.AddContactPoints(Options.Default.IP_PREFIX + "1").Build();
                return Cluster;
            }

            public static ISession Connect(string keyspace = null)
            {
                int tryNo = 0;
            RETRY:
                try
                {
                    Session = Cluster.Connect();
                    if (keyspace != null)
                    {
                        Session.CreateKeyspaceIfNotExists(keyspace);
                        Session.ChangeKeyspace(keyspace);
                    }
                    return Session;
                }
                catch (NoHostAvailableException e)
                {
                    if (tryNo < 10)
                    {
                        Trace.TraceInformation("CannotConnect to CCM node - give another try");
                        tryNo++;
                        Thread.Sleep(1000);
                        goto RETRY;
                    }
                    foreach (var entry in e.Errors)
                        Trace.TraceError("Error connecting to " + entry.Key + ": " + entry.Value);
                    throw new InvalidOperationException(null, e);
                }
            }

            public static void Drop()
            {
                if (Session != null && Session.Keyspace != null)
                    Session.DeleteKeyspaceIfExists(Session.Keyspace);
                Cluster.Shutdown();
            }

            public static void Shutdown()
            {
                Cluster.Shutdown();
                Session = null;
            }
        }



    }
}
