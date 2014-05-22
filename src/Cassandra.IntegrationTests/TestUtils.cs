//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Cassandra.IntegrationTests
{
    /// <summary>
    ///  A number of static fields/methods handy for tests.
    /// </summary>
    public static class TestUtils
    {
        private static readonly Logger logger = new Logger(typeof (TestUtils));

        public static readonly string CREATE_KEYSPACE_SIMPLE_FORMAT =
            "CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {1} }}";

        public static readonly string CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";

        public static readonly string SIMPLE_KEYSPACE = "ks";
        public static readonly string SIMPLE_TABLE = "test";

        public static readonly string CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";
        public const string CREATE_TABLE_ALL_TYPES = @"
            create table {0} (
            id uuid primary key,
            text_sample text,
            int_sample int,
            bigint_sample bigint,
            float_sample float,
            double_sample double,
            decimal_sample decimal,
            blob_sample blob,
            boolean_sample boolean,
            timestamp_sample timestamp,
            inet_sample inet);
        ";
        public const string CREATE_TABLE_TIME_SERIES = @"
            create table {0} (
            id uuid,
            event_time timestamp,
            text_sample text,
            int_sample int,
            bigint_sample bigint,
            float_sample float,
            double_sample double,
            decimal_sample decimal,
            blob_sample blob,
            boolean_sample boolean,
            timestamp_sample timestamp,
            inet_sample inet,
            PRIMARY KEY(id, event_time));
        ";

        public static readonly string INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
        public static readonly string SELECT_ALL_FORMAT = "SELECT * FROM {0}";
        public static readonly string SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";

        /// <summary>
        /// Determines if the test should use a remote ccm instance
        /// </summary>
        public static bool UseRemoteCcm
        {
            get
            {
                return ConfigurationManager.AppSettings["UseRemote"] == "true";
            }
        }

        // Wait for a node to be up and running
        // This is used because there is some delay between when a node has been
        // added through ccm and when it's actually available for querying'
        public static void waitFor(string node, Cluster cluster, int maxTry)
        {
            waitFor(node, cluster, maxTry, false, false);
        }

        public static void waitForDown(string node, Cluster cluster, int maxTry)
        {
            waitFor(node, cluster, maxTry, true, false);
        }

        public static void waitForDecommission(string node, Cluster cluster, int maxTry)
        {
            waitFor(node, cluster, maxTry, true, true);
        }

        public static void waitForDownWithWait(String node, Cluster cluster, int waitTime)
        {
            waitFor(node, cluster, 60, true, false);

            // FIXME: Once stop() works, remove this line
            try
            {
                Thread.Sleep(waitTime*1000);
            }
            catch (InvalidQueryException e)
            {
                Debug.Write(e.StackTrace);
            }
        }

        private static void waitFor(string node, Cluster cluster, int maxTry, bool waitForDead, bool waitForOut)
        {
            // In the case where the we've killed the last node in the cluster, if we haven't
            // tried doing an actual query, the driver won't realize that last node is dead until'
            // keep alive kicks in, but that's a fairly long time. So we cheat and trigger a force'
            // the detection by forcing a request.
            bool disconnected = false;
            if (waitForDead || waitForOut)
                disconnected = !cluster.RefreshSchema(null, null);

            if (disconnected)
                return;

            IPAddress address;
            try
            {
                address = IPAddress.Parse(node);
            }
            catch (Exception)
            {
                // That's a problem but that's not *our* problem
                return;
            }

            Metadata metadata = cluster.Metadata;
            for (int i = 0; i < maxTry; ++i)
            {
                bool found = false;
                foreach (Host host in metadata.AllHosts())
                {
                    if (host.Address.Equals(address))
                    {
                        found = true;
                        if (testHost(host, waitForDead))
                            return;
                    }
                }
                if (waitForDead && !found)
                    return;
                try
                {
                    Thread.Sleep(1000);
                }
                catch (Exception)
                {
                }
            }

            foreach (Host host in metadata.AllHosts())
            {
                if (host.Address.Equals(address))
                {
                    if (testHost(host, waitForDead))
                    {
                        return;
                    }
                    // logging it because this give use the timestamp of when this happens
                    logger.Info(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                    throw new InvalidOperationException(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                }
            }

            if (waitForOut)
            {
            }
            logger.Info(node + " is not part of the cluster after " + maxTry + "s");
            throw new InvalidOperationException(node + " is not part of the cluster after " + maxTry + "s");
        }

        private static bool testHost(Host host, bool testForDown)
        {
            return testForDown ? !host.IsUp : host.IsConsiderablyUp;
        }

        /// <summary>
        /// Executes a python command
        /// </summary>
        public static ProcessOutput ExecutePythonCommand(string pythonArgs, int timeout = 300000)
        {
            var output = new ProcessOutput();
            using (var process = new Process())
            {
                process.StartInfo.FileName = "python.exe";
                process.StartInfo.Arguments = pythonArgs;
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

        public static ProcessOutput ExecuteLocalCcm(string ccmArgs, string ccmConfigDir, int timeout = 300000)
        {
            var ccmPath = ConfigurationManager.AppSettings["CcmPath"];
            if (ccmPath == null)
            {
                //By convention
                ccmPath = Path.Combine(Environment.GetEnvironmentVariable("USERPROFILE"), @"workspace\\tools\\ccm");
            }
            ccmPath = Path.Combine(ccmPath, "ccm");
            if (!FileExists(ccmPath))
            {
                return new ProcessOutput()
                {
                    ExitCode = 1000,
                    OutputText = new StringBuilder("Ccm file does not exists in path" + ccmPath)
                };
            }
            ccmPath = EscapePath(ccmPath);
            ccmConfigDir = EscapePath(ccmConfigDir);
            ccmArgs += " --config-dir=" + ccmConfigDir;
            Trace.TraceInformation("Executing ccm: " + ccmArgs);
            return ExecutePythonCommand(ccmPath + " " + ccmArgs, timeout);
        }

        /// <summary>
        /// Starts a Cassandra cluster with the name, version and amount of nodes provided.
        /// </summary>
        /// <param name="ccmConfigDir">Path to the location where the cluster will be created</param>
        /// <param name="cassandraVersion">Cassandra version in the form of MAJOR.MINOR.PATCH semver</param>
        /// <param name="nodeLength">amount of nodes in the cluster</param>
        /// <param name="secondDcNodeLength">amount of nodes to add the second DC</param>
        /// <param name="clusterName"></param>
        /// <returns></returns>
        public static ProcessOutput ExecuteLocalCcmClusterStart(string ccmConfigDir,string cassandraVersion, int nodeLength = 1, int secondDcNodeLength = 0, string clusterName = "test")
        {
            //Starting ccm cluster involves:
            //  1.- Getting the Apache Cassandra Distro
            //  2.- Compiling it
            //  3.- Fill the config files
            //  4.- Starting each node.

            //Considerations: 
            //  As steps 1 and 2 can take a while, try to fail fast (2 sec) by doing a "ccm list"
            //  Also, the process can exit before the nodes are actually up: Execute ccm status until they are up

            var totalNodeLength = nodeLength + secondDcNodeLength;

            //Only if ccm list succedes, create the cluster and continue.
            var output = TestUtils.ExecuteLocalCcm("list", ccmConfigDir, 2000);
            if (output.ExitCode != 0)
            {
                return output;
            }

            var ccmCommand = String.Format("create {0} -v {1}", clusterName, cassandraVersion);
            //When creating a cluster, it could download the Cassandra binaries from the internet.
            //Give enough time = 3 minutes.
            var timeout = 180000;
            output = TestUtils.ExecuteLocalCcm(ccmCommand, ccmConfigDir, timeout);
            if (output.ExitCode != 0)
            {
                return output;
            }
            if (secondDcNodeLength > 0)
            {
                ccmCommand = String.Format("populate -n {0}:{1}", nodeLength, secondDcNodeLength); 
            }
            else
            {
                ccmCommand = "populate -n " + nodeLength;
            }
            var populateOutput = TestUtils.ExecuteLocalCcm(ccmCommand, ccmConfigDir, 300000);
            if (populateOutput.ExitCode != 0)
            {
                return populateOutput;
            }
            output.OutputText.AppendLine(populateOutput.ToString());
            var startOutput = TestUtils.ExecuteLocalCcm("start", ccmConfigDir);
            if (startOutput.ExitCode != 0)
            {
                return startOutput;
            }
            output.OutputText.AppendLine(startOutput.ToString());

            //Nodes are starting, but we dont know for sure if they are have started.
            var allNodesAreUp = false;
            var safeCounter = 0;
            while (!allNodesAreUp && safeCounter < 10)
            {
                var statusOutput = TestUtils.ExecuteLocalCcm("status", ccmConfigDir, 1000);
                if (statusOutput.ExitCode != 0)
                {
                    //Something went wrong
                    output = statusOutput;
                    break;
                }
                //Analyze the status output to see if all nodes are up
                if (Regex.Matches(statusOutput.OutputText.ToString(), "UP", RegexOptions.Multiline).Count == totalNodeLength)
                {
                    //All nodes are up
                    for (int x = 1; x <= totalNodeLength; x++)
                    {
                        var foundText = false;
                        var sw = new Stopwatch();
                        sw.Start();
                        while (sw.ElapsedMilliseconds < 180000)
                        {
                            var logFileText =
                                TryReadAllTextNoLock(Path.Combine(ccmConfigDir, clusterName, String.Format("node{0}\\logs\\system.log", x)));
                            if (Regex.IsMatch(logFileText, "listening for CQL clients", RegexOptions.Multiline))
                            {
                                foundText = true;
                                break;
                            }
                        }
                        if (!foundText)
                        {
                            throw new TestInfrastructureException(String.Format("node{0} did not properly start", x));
                        }
                    }
                    allNodesAreUp = true;
                }
                safeCounter++;
            }

            return output;
        }

        /// <summary>
        /// Stops the cluster and removes the config files
        /// </summary>
        /// <returns></returns>
        public static ProcessOutput ExecuteLocalCcmClusterRemove(string ccmConfigDir)
        {
            return TestUtils.ExecuteLocalCcm("remove", ccmConfigDir);
        }

        /// <summary>
        /// Reads a text file without file locking
        /// </summary>
        /// <returns></returns>
        public static string TryReadAllTextNoLock(string fileName)
        {
            string fileText = "";
            try
            {
                using (var file = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var reader = new StreamReader(file))
                    {
                        fileText = reader.ReadToEnd();
                    }
                }

            }
            catch
            {
                //We tried and failed, dont mind
            }
            return fileText;
        }

        private static Dictionary<string, bool> _existsCache = new Dictionary<string,bool>();
        /// <summary>
        /// Checks that the file exists and caches the result in a static variable
        /// </summary>
        public static bool FileExists(string path)
        {
            if (!_existsCache.ContainsKey(path))
            {
                _existsCache[path] = File.Exists(path);
            }
            return _existsCache[path];
        }

        /// <summary>
        /// Adds double quotes to the path in case it contains spaces.
        /// </summary>
        public static string EscapePath(string path)
        {
            if (path == null)
            {
                throw new ArgumentNullException("path");
            }
            if (path.Contains(" "))
            {
                return "\"" + path + "\"";
            }
            return path;
        }

        /// <summary>
        /// Create a temporary directory inside OS temp path and returns the name of path of the newly created directory
        /// </summary>
        /// <returns></returns>
        public static string CreateTempDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), "ccm-" + Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }

        public static CcmClusterInfo CcmSetup(int nodeLength, Builder builder = null, string keyspaceName = null, int secondDcNodeLength = 0)
        {
            var clusterInfo = new CcmClusterInfo();
            if (builder == null)
            {
                builder = Cluster.Builder();
            }
            if (UseRemoteCcm)
            {
                CCMBridge.ReusableCCMCluster.Setup(nodeLength);
                clusterInfo.Cluster = CCMBridge.ReusableCCMCluster.Build(builder);
                if (keyspaceName != null)
                {
                    clusterInfo.Session = CCMBridge.ReusableCCMCluster.Connect(keyspaceName);
                }
            }
            else
            {
                //Create a local instance
                clusterInfo.ConfigDir = TestUtils.CreateTempDirectory();
                var output = TestUtils.ExecuteLocalCcmClusterStart(clusterInfo.ConfigDir, Options.Default.CASSANDRA_VERSION, nodeLength, secondDcNodeLength);

                if (output.ExitCode != 0)
                {
                    throw new TestInfrastructureException("Local ccm could not start: " + output.ToString());
                }
                clusterInfo.Cluster = builder
                    .AddContactPoint("127.0.0.1")
                    .Build();
                clusterInfo.Session = clusterInfo.Cluster.Connect();
                if (keyspaceName != null)
                {
                    clusterInfo.Session.CreateKeyspaceIfNotExists(keyspaceName);
                    clusterInfo.Session.ChangeKeyspace(keyspaceName);
                }
            }
            return clusterInfo;
        }

        public static void CcmRemove(CcmClusterInfo info)
        {
            if (UseRemoteCcm)
            {
                CCMBridge.ReusableCCMCluster.Drop();
            }
            else
            {
                //Remove the cluster
                TestUtils.ExecuteLocalCcmClusterRemove(info.ConfigDir);
            }
        }

        /// <summary>
        /// Starts a node
        /// </summary>
        /// <param name="info"></param>
        /// <param name="n"></param>
        public static void CcmStart(CcmClusterInfo info, int n)
        {
            var cmd = string.Format("node{0} start", n);
            ExecuteLocalCcm(cmd, info.ConfigDir, 5000);
        }

        /// <summary>
        /// Stops a node in the cluster with the provided index (1 based)
        /// </summary>
        public static void CcmStopNode(CcmClusterInfo info, int n)
        {
            var cmd = string.Format("node{0} stop", n);
            ExecuteLocalCcm(cmd, info.ConfigDir, 2000);
        }

        /// <summary>
        /// Stops a node (not gently) in the cluster with the provided index (1 based)
        /// </summary>
        public static void CcmStopForce(CcmClusterInfo info, int n)
        {
            ExecuteLocalCcm(string.Format("node{0} stop --not-gently", n), info.ConfigDir, 2000);
        }

        public static void CcmBootstrapNode(CcmClusterInfo info, int node, string dc = null)
        {
            if (dc == null)
            {
                ExecuteLocalCcm(string.Format("add node{0} -i {1}{2} -j {3} -b", node, Options.Default.IP_PREFIX, node, 7000 + 100 * node), info.ConfigDir);
            }
            else
            {
                ExecuteLocalCcm(string.Format("add node{0} -i {1}{2} -j {3} -b -d {4}", node, Options.Default.IP_PREFIX, node, 7000 + 100 * node, dc), info.ConfigDir);
            }
        }

        public static void CcmDecommissionNode(CcmClusterInfo info, int node)
        {
            ExecuteLocalCcm(string.Format("node{0} decommission", node), info.ConfigDir);
        }
    }

    /// <summary>
    /// Represents a result from executing an external process.
    /// </summary>
    public class ProcessOutput
    {
        public int ExitCode { get; set; }

        public StringBuilder OutputText { get; set; }

        public ProcessOutput()
        {
            OutputText = new StringBuilder();
            ExitCode = Int32.MinValue;
        }

        public override string ToString()
        {
            return
                "Exit Code: " + this.ExitCode + Environment.NewLine +
                "Output Text: " + this.OutputText.ToString() + Environment.NewLine;
        }
    }

    public class CcmClusterInfo
    {
        public Cluster Cluster { get; set; }

        public ISession Session { get; set; }

        public string ConfigDir { get; set; }
    }
}