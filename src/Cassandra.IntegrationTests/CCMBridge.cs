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
﻿using System;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests;


namespace Cassandra
{

    public class CCMBridge : IDisposable
    {
        static CCMBridge()
        {

        }

        private readonly DirectoryInfo _ccmDir;
        private Renci.SshNet.SshClient _ssh_client;
        private Renci.SshNet.ShellStream _ssh_shellStream;
        private Options trick = Options.Default;

        private CCMBridge()
        {
            _ccmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            _ssh_client = new Renci.SshNet.SshClient(Options.Default.SSH_HOST, Options.Default.SSH_PORT, Options.Default.SSH_USERNAME, Options.Default.SSH_PASSWORD);
            _ssh_client.Connect();

            _ssh_shellStream = _ssh_client.CreateShellStream("CCM", 80, 60, 100, 100, 1000);
            var outp = new StringBuilder();
            while (true)
            {
                outp.Append(_ssh_shellStream.Read());
                if (outp.ToString().Trim().EndsWith("$"))
                    break;
            }
        }

        public void Dispose()
        {
            if (_ssh_client != null)
            {
                _ssh_client.Disconnect();
                _ssh_client = null;
            }
        }

        ~CCMBridge()
        {
            if (_ssh_client != null)
                _ssh_client.Disconnect();
        }

        public static CCMBridge Create(string name)
        {
            CCMBridge bridge = new CCMBridge();
            bridge.ExecuteCCM(string.Format("Create {0} -b -i {1} -v {2}", name, Options.Default.IP_PREFIX, Options.Default.CASSANDRA_VERSION));
            return bridge;
        }

        public static CCMBridge Create(string name, int nbNodes, bool useAlreadyExisting= false)
        {
#if !MYTEST
            if (!useAlreadyExisting && (nbNodes > 6))
                throw new InvalidOperationException();
#endif

            CCMBridge bridge = new CCMBridge();
            bridge.ExecuteCCM(string.Format("Create {0} -n {1} -s -i {2} -b -v {3}", name, nbNodes, Options.Default.IP_PREFIX, Options.Default.CASSANDRA_VERSION), useAlreadyExisting);
            return bridge;
        }

        public static CCMBridge Create(string name, int nbNodesDC1, int nbNodesDC2, bool useAlreadyExisting =false)
        {
#if !MYTEST
            if (!useAlreadyExisting && (nbNodesDC1 + nbNodesDC2 > 6))
                throw new InvalidOperationException();
#endif

            CCMBridge bridge = new CCMBridge();
            bridge.ExecuteCCM(string.Format("Create {0} -n {1}:{2} -s -i {3} -b -v {4}", name, nbNodesDC1, nbNodesDC2, Options.Default.IP_PREFIX, Options.Default.CASSANDRA_VERSION), useAlreadyExisting);
            return bridge;
        }

        public void Start()
        {
            ExecuteCCM("start");
        }

        public void Stop()
        {
            ExecuteCCM("stop");
        }

        public void ForceStop()
        {
            ExecuteCCM("stop --not-gently");
        }

        public void Start(int n)
        {
            ExecuteCCM(string.Format("node{0} start", n));
        }

        public void Stop(int n)
        {
            ExecuteCCM(string.Format("node{0} stop", n));
        }

        public void ForceStop(int n)
        {
            ExecuteCCM(string.Format("node{0} stop --not-gently", n));
        }

        public void Remove()
        {
            Stop();
            ExecuteCCM(string.Format("remove"));
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
#if !MYTEST
            if (n > 5)
                throw new InvalidOperationException();
#endif

            if (dc == null)
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b", n, Options.Default.IP_PREFIX, n, 7000 + 100 * n));
            else
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b -d {4}", n, Options.Default.IP_PREFIX, n, 7000 + 100 * n, dc));
            ExecuteCCM(string.Format("node{0} start", n));
        }

        public void DecommissionNode(int n)
        {
            ExecuteCCM(string.Format("node{0} decommission", n));
        }

        private int dead = 0;

        private void ExecuteCCM(string args, bool useAlreadyExisting = false)
        {
            Trace.TraceInformation("CCM>"+args);
            if (_ssh_shellStream.DataAvailable)
                _ssh_shellStream.Read();
            _ssh_shellStream.WriteLine("ccm " + args /*+ " --config-dir=" + _ccmDir*/);
            var outp = new StringBuilder();
            while (true)
            {
                var txt = _ssh_shellStream.Read();
                outp.Append(txt);
                if (txt.Contains("$"))
                    break;
            }
            if (outp.ToString().Contains("[Errno"))
            {
                if (outp.ToString().Contains("[Errno 17]") && dead<2)
                {
                    if (useAlreadyExisting)
                        return;
                    dead++;
                    ExecuteCCMAndPrint("remove test");
                    PureExecute("killall java");
                    Thread.Sleep(5000);
                    ReusableCCMCluster.Reset();
                    ExecuteCCM(args);
                    return;
                }
                var lines = outp.ToString().Split('\n');
                for (int i = 0; i < lines.Length; i++)
                    Trace.TraceError("err>" + lines[i].Trim());
                throw new InvalidOperationException();
            }
            dead = 0;
            Thread.Sleep(2000);
        }

        private void ExecuteCCMAndPrint(string args)
        {
            Trace.TraceInformation("CCM>"+args);
            _ssh_shellStream.WriteLine("ccm " + args /*+ " --config-dir=" + _ccmDir*/);
            var outp = new StringBuilder();
            while (true)
            {
                var txt = _ssh_shellStream.Read();
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
            _ssh_shellStream.WriteLine(args);
            var outp = new StringBuilder();
            while (true)
            {
                var txt = _ssh_shellStream.Read();
                outp.Append(txt);
                if (txt.Contains("$"))
                    break;
            }
        }

        // One cluster for the whole test class
        public abstract class PerClassSingleNodeCluster
        {

            protected static CCMBridge cassandraCluster;
            private static bool erroredOut;
            private static bool schemaCreated;

            protected static Cluster cluster;
            protected static ISession session;

            protected abstract ICollection<string> GetTableDefinitions();

            public void ErrorOut()
            {
                erroredOut = true;
            }

            public static void CreateCluster()
            {
                erroredOut = false;
                schemaCreated = false;
                cassandraCluster = CCMBridge.Create("test", 1);
                try
                {
                    var builder = Cluster.Builder().AddContactPoints(Options.Default.IP_PREFIX + "1");
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

                    cluster = builder.Build();
                    session = cluster.Connect();
                }
                catch (NoHostAvailableException e)
                {
                    erroredOut = true;
                    foreach (var entry in e.Errors)
                        Trace.TraceError("Error connecting to " + entry.Key + ": " + entry.Value);
                    throw new InvalidOperationException("", e);
                }
            }

            public static void DiscardCluster()
            {
                if (cluster != null)
                    cluster.Shutdown();

                if (cassandraCluster == null)
                {
                    Trace.TraceError("No cluster to discard");
                }
                else if (erroredOut)
                {
                    cassandraCluster.Stop();
                    Trace.TraceInformation("Error during tests, kept C* logs in " + cassandraCluster._ccmDir);
                }
                else
                {
                    cassandraCluster.Remove();
                    cassandraCluster._ccmDir.Delete();
                }
            }

            public void BeforeClass()
            {
                CreateCluster();
                MaybeCreateSchema();
            }

            public void MaybeCreateSchema()
            {

                try
                {
                    if (schemaCreated)
                        return;

                    try
                    {
                        session.Execute(String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, TestUtils.SIMPLE_KEYSPACE, 1));
                    }
                    catch (AlreadyExistsException)
                    {
                        // It's ok, ignore'
                    }

                    session.Execute("USE " + TestUtils.SIMPLE_KEYSPACE);

                    foreach (string tableDef in GetTableDefinitions())
                    {
                        try
                        {
                            session.Execute(tableDef);
                        }
                        catch (AlreadyExistsException)
                        {
                            // It's ok, ignore'
                        }
                    }

                    schemaCreated = true;
                }
                catch (DriverException e)
                {
                    erroredOut = true;
                    throw e;
                }
            }
        }
        public static class ReusableCCMCluster
        {
            static int NbNodesDC1;
            static int NbNodesDC2;
            public static CCMBridge CCMBridge;

            internal static void Reset()
            {
                NbNodesDC1 = 0;
                NbNodesDC2 = 0;
            }

            public static void Setup(int nbNodesDC1, int nbNodesDC2 = 0,bool useAlreadyExisting = false)
            {
                if (nbNodesDC2 == 0)
                {
                    if (nbNodesDC1 != NbNodesDC1)
                    {
                        Trace.TraceInformation("Cassandra:" + Options.Default.CASSANDRA_VERSION);
                        CCMBridge = CCMBridge.Create("test", nbNodesDC1, useAlreadyExisting);
                        NbNodesDC1 = nbNodesDC1;
                        NbNodesDC2 = 0;
                    }
                }
                else
                {
                    if (nbNodesDC1 != NbNodesDC1 || nbNodesDC2 != NbNodesDC2)
                    {
                        CCMBridge = CCMBridge.Create("test", nbNodesDC1, nbNodesDC2, useAlreadyExisting);
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

        public class CCMCluster
        {

            public readonly Cluster Cluster;
            public readonly ISession Session;

            public readonly CCMBridge CCMBridge;

            private bool erroredOut;

            public static CCMCluster Create(int nbNodes, Builder builder)
            {
#if !MYTEST
                if (nbNodes > 6)
                    throw new InvalidOperationException();
#endif
                if (nbNodes == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodes), builder);
            }

            public static CCMCluster Create(int nbNodesDC1, int nbNodesDC2, Builder builder)
            {
#if !MYTEST
                if (nbNodesDC1 + nbNodesDC2 > 6)
                    throw new InvalidOperationException();
#endif
                if (nbNodesDC1 == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodesDC1, nbNodesDC2), builder);
            }

            private CCMCluster(CCMBridge ccmBridge, Builder builder)
            {
                int tryNo = 0;
                builder.AddContactPoints(Options.Default.IP_PREFIX + "1");
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

                this.Cluster = builder.Build();
            RETRY:
                this.CCMBridge = ccmBridge;
                try
                {
                    this.Session = Cluster.Connect();
                    if(tryNo>0)
                        Cluster.RefreshSchema();
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

            public void ErrorOut()
            {
                erroredOut = true;
            }

            public void Discard()
            {
                if (Cluster != null)
                    Cluster.Shutdown();

                if (CCMBridge == null)
                {
                    Trace.TraceError("No cluster to discard");
                }
                else if (erroredOut)
                {
                    CCMBridge.Stop();
                    Trace.TraceInformation("Error during tests, kept C* logs in " + CCMBridge._ccmDir);
                }
                else
                {
                    CCMBridge.Remove();
                    try
                    {
                        CCMBridge._ccmDir.Delete();
                    }
                    catch { }
                }
            }
        }


    }
}