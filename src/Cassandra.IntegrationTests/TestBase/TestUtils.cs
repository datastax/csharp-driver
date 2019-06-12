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
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Cassandra.IntegrationTests.Core;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.TestBase
{
    /// <summary>
    ///  A number of static fields/methods handy for tests.
    /// </summary>
    internal static class TestUtils
    {
        private const int DefaultSleepIterationMs = 1000;

        public static readonly string CreateKeyspaceSimpleFormat =
            "CREATE KEYSPACE \"{0}\" WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {1} }}";

        public static readonly string CreateKeyspaceGenericFormat = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";

        public static readonly string CreateTableSimpleFormat = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";

        public const string CreateTableAllTypes = @"
            create table {0} (
            id uuid primary key,
            ascii_sample ascii,
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
            timeuuid_sample timeuuid,
            map_sample map<text, text>,
            list_sample list<text>,
            set_sample set<text>);
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

        public static string GetTestClusterNameBasedOnTime()
        {
            return "test_" + (DateTimeOffset.UtcNow.Ticks / TimeSpan.TicksPerSecond);
        }

        public static string GetUniqueKeyspaceName()
        {
            return "TestKeySpace_" + Randomm.RandomAlphaNum(12);
        }

        public static string GetUniqueTableName()
        {
            return "TestTable_" + Randomm.RandomAlphaNum(12);
        }

        public static void TryToDeleteKeyspace(ISession session, string keyspaceName)
        {
            if (session != null)
                session.DeleteKeyspaceIfExists(keyspaceName);
        }

        public static bool TableExists(ISession session, string keyspaceName, string tableName, bool caseSensitive=false)
        {
            var cql = caseSensitive ? string.Format(@"SELECT * FROM ""{0}"".""{1}"" LIMIT 1", keyspaceName, tableName) 
                : string.Format("SELECT * FROM {0}.{1} LIMIT 1", keyspaceName, tableName);
            //it will throw a InvalidQueryException if the table/keyspace does not exist
            session.Execute(cql);
            return true;
        }

        public static byte[] GetBytes(string str)
        {
            byte[] bytes = new byte[str.Length*sizeof (char)];
            System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        /// <summary>
        /// Validates that the bootstrapped node was added to the cluster and was queried.
        /// </summary>
        public static void ValidateBootStrappedNodeIsQueried(ITestCluster testCluster, int expectedTotalNodeCount, string newlyBootstrappedHost)
        {
            var hostsQueried = new List<string>();
            DateTime timeInTheFuture = DateTime.Now.AddSeconds(120);
            while (testCluster.Cluster.Metadata.AllHosts().ToList().Count() < expectedTotalNodeCount && DateTime.Now < timeInTheFuture)
            {
                var rs = testCluster.Session.Execute("SELECT key FROM system.local");
                hostsQueried.Add(rs.Info.QueriedHost.Address.ToString());
                Thread.Sleep(500);
            }
            Assert.That(testCluster.Cluster.Metadata.AllHosts().ToList().Count, Is.EqualTo(expectedTotalNodeCount));
            timeInTheFuture = DateTime.Now.AddSeconds(120);
            while (!hostsQueried.Contains(newlyBootstrappedHost) && DateTime.Now < timeInTheFuture)
            {
                var rs = testCluster.Session.Execute("SELECT key FROM system.local");
                hostsQueried.Add(rs.Info.QueriedHost.Address.ToString());
                Thread.Sleep(500);
            }
            // Validate host was queried
            Assert.True(hostsQueried.Any(ip => ip.ToString() == newlyBootstrappedHost), "Newly bootstrapped node was not queried!");
        }

        /// <summary>
        /// Determines if the test should use a remote ccm instance
        /// </summary>
        public static bool UseRemoteCcm
        {
            get { return false; }
        }

        public static void WaitForUp(string nodeHost, int nodePort, int maxSecondsToKeepTrying)
        {
            int msSleepPerIteration = 500;
            DateTime futureDateTime = DateTime.Now.AddSeconds(maxSecondsToKeepTrying);
            while (DateTime.Now < futureDateTime)
            {
                if (IsNodeReachable(IPAddress.Parse(nodeHost), nodePort))
                {
                    return;
                }
                Trace.TraceInformation(
                    string.Format("Still waiting for node host: {0} to be available for connection, " +
                        " waiting another {1} MS ... ", nodeHost + ":" + nodePort, msSleepPerIteration));
                Thread.Sleep(msSleepPerIteration);
            }
            throw new Exception("Could not connect to node: " + nodeHost + ":" + nodePort + " after " + maxSecondsToKeepTrying + " seconds!");
        }

        private static void WaitForMeta(string nodeHost, Cluster cluster, int maxTry, bool waitForUp)
        {
            string expectedFinalNodeState = "UP";
            if (!waitForUp)
                expectedFinalNodeState = "DOWN";
            for (int i = 0; i < maxTry; ++i)
            {
                try
                {
                    // Are all nodes in the cluster accounted for?
                    bool disconnected = !cluster.RefreshSchema();
                    if (disconnected)
                    {
                        string warnStr = "While waiting for host " + nodeHost + " to be " + expectedFinalNodeState + ", the cluster is now totally down, returning now ... ";
                        Trace.TraceWarning(warnStr);
                        return;
                    }

                    Metadata metadata = cluster.Metadata;
                    foreach (Host host in metadata.AllHosts())
                    {
                        bool hostFound = false;
                        if (host.Address.ToString() == nodeHost)
                        {
                            hostFound = true;
                            if (host.IsUp && waitForUp)
                            {
                                Trace.TraceInformation("Verified according to cluster meta that host " + nodeHost + " is " + expectedFinalNodeState + ", returning now ... ");
                                return;
                            }
                            Trace.TraceWarning("We're waiting for host " + nodeHost + " to be " + expectedFinalNodeState);
                        }
                        // Is the host even in the meta list?
                        if (!hostFound)
                        {
                            if (!waitForUp)
                            {
                                Trace.TraceInformation("Verified according to cluster meta that host " + host.Address + " is not available in the MetaData hosts list, returning now ... ");
                                return;
                            }
                            else
                                Trace.TraceWarning("We're waiting for host " + nodeHost + " to be " + expectedFinalNodeState + ", but this host was not found in the MetaData hosts list!");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("None of the hosts tried for query are available") && !waitForUp)
                    {
                        Trace.TraceInformation("Verified according to cluster meta that host " + nodeHost + " is not available in the MetaData hosts list, returning now ... ");
                        return;
                    }
                    Trace.TraceInformation("Exception caught while waiting for meta data: " + e.Message);
                }
                Trace.TraceWarning("Waiting for node host: " + nodeHost + " to be " + expectedFinalNodeState);
                Thread.Sleep(DefaultSleepIterationMs);
            }
            string errStr = "Node host should have been " + expectedFinalNodeState + " but was not after " + maxTry + " tries!";
            Trace.TraceError(errStr);
        }

        public static void WaitFor(string node, Cluster cluster, int maxTry)
        {
            WaitFor(node, cluster, maxTry, false, false);
        }

        public static void WaitForDown(string node, Cluster cluster, int maxTry)
        {
            WaitFor(node, cluster, maxTry, true, false);
        }

        public static void waitForDecommission(string node, Cluster cluster, int maxTry)
        {
            WaitFor(node, cluster, maxTry, true, true);
        }

        public static void WaitForDownWithWait(String node, Cluster cluster, int waitTime)
        {
            WaitFor(node, cluster, 90, true, false);

            // FIXME: Once stop() works, remove this line
            try
            {
                Thread.Sleep(waitTime * 1000);
            }
            catch (InvalidQueryException e)
            {
                Debug.Write(e.StackTrace);
            }
        }

        private static void WaitFor(string node, Cluster cluster, int maxTry, bool waitForDead, bool waitForOut)
        {
            WaitForMeta(node, cluster, maxTry, !waitForDead); 
        }

        /// <summary>
        /// Spawns a new process (platform independent)
        /// </summary>
        public static ProcessOutput ExecuteProcess(string processName, string args, int timeout = 300000)
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

        public static ProcessOutput ExecuteLocalCcm(string ccmArgs, string ccmConfigDir, int timeout = 300000, bool throwOnProcessError = false)
        {
            ccmConfigDir = EscapePath(ccmConfigDir);
            var args = ccmArgs + " --config-dir=" + ccmConfigDir;
            Trace.TraceInformation("Executing ccm: " + ccmArgs);
            var processName = "/usr/local/bin/ccm";
            if (IsWin)
            {
                processName = "cmd.exe";
                args = "/c ccm " + args;
            }
            var output = ExecuteProcess(processName, args, timeout);
            if (throwOnProcessError)
            {
                ValidateOutput(output);
            }
            return output;
        }

        public static bool IsWin
        {
            get { return TestHelper.IsWin; }
        }

        private static void ValidateOutput(ProcessOutput output)
        {
            if (output.ExitCode != 0)
            {
                throw new TestInfrastructureException(string.Format("Process exited in error {0}", output.ToString()));
            }
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
        public static ProcessOutput ExecuteLocalCcmClusterStart(string ccmConfigDir, string cassandraVersion, int nodeLength = 1, int secondDcNodeLength = 0, string clusterName = "test")
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
            var output = TestUtils.ExecuteLocalCcm("stop", ccmConfigDir);
            if (output.ExitCode != 0)
            {
                return output;
            }
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

        private static Dictionary<string, bool> _existsCache = new Dictionary<string, bool>();
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
            var tempDirectory = Path.Combine(Path.GetTempPath(), "ccm-" + Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }

        //public static void CcmBootstrapNode(CcmCluster ccmCluster, int node, string dc = null)
        //{
        //    ProcessOutput output = null;
        //    if (dc == null)
        //    {
        //        output = ccmCluster.CcmBridge.ExecuteCcm(string.Format("add node{0} -i {1}{2} -j {3} -b", node, Options.Default.IP_PREFIX, node, 7000 + 100 * node));
        //    }
        //    else
        //    {
        //        output = ccmCluster.CcmBridge.ExecuteCcm(string.Format("add node{0} -i {1}{2} -j {3} -b -d {4}", node, Options.Default.IP_PREFIX, node, 7000 + 100 * node, dc));
        //    }
        //    if (output.ExitCode != 0)
        //    {
        //        throw new TestInfrastructureException("Local ccm could not add node: " + output.ToString());
        //    }
        //}

        public static void CcmDecommissionNode(CcmClusterInfo info, int node)
        {
            ExecuteLocalCcm(string.Format("node{0} decommission", node), info.ConfigDir);
        }

        /// <summary>
        /// Determines if a connection can be made to a node at port 9042
        /// </summary>
        public static bool IsNodeReachable(IPAddress ip, int port = 9042)
        {
            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                try
                {
                    socket.Connect(new IPEndPoint(ip, port));
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }
        
        public static void WaitForSchemaAgreement(
            ICluster cluster, bool ignoreDownNodes = true, bool throwOnMaxRetries = false, int maxRetries = 20)
        {
            var hostsLength = cluster.AllHosts().Count;
            if (hostsLength == 1)
            {
                return;
            }
            var cc = cluster.Metadata.ControlConnection;
            var counter = 0;
            var nodesDown = ignoreDownNodes ? cluster.AllHosts().Count(h => !h.IsConsiderablyUp) : 0;
            while (counter++ < maxRetries)
            {
                Trace.TraceInformation("Waiting for test schema agreement");
                Thread.Sleep(500);
                var schemaVersions = new List<Guid>();
                //peers
                schemaVersions.AddRange(cc.Query("SELECT peer, schema_version FROM system.peers").Select(r => r.GetValue<Guid>("schema_version")));
                //local
                schemaVersions.Add(cc.Query("SELECT schema_version FROM system.local").Select(r => r.GetValue<Guid>("schema_version")).First());

                var differentSchemas = schemaVersions.Distinct().Count();
                if (differentSchemas <= 1 + nodesDown)
                {
                    //There is 1 schema version or 1 + nodes that are considered as down
                    return;
                }
            }

            if (throwOnMaxRetries)
            {
                throw new Exception("Reached max attempts for obtaining a single schema version from all nodes.");
            }
        }

        public static void WaitForSchemaAgreement(CcmClusterInfo clusterInfo)
        {
            WaitForSchemaAgreement(clusterInfo.Cluster);
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
