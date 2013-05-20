using System;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Text;


namespace Cassandra
{

    public class CCMBridge : IDisposable
    {

        public static readonly string IP_PREFIX;

        private static readonly string CASSANDRA_VERSION_REGEXP = "\\d\\.\\d\\.\\d(-\\w+)?";

        private static readonly string CASSANDRA_DIR;
        private static readonly string CASSANDRA_VERSION;

        static CCMBridge()
        {
#if MYTEST
            var version = MyTest.MyTestOptions.Default.CassandraVersion;
            IP_PREFIX = MyTest.MyTestOptions.Default.IpPrefix;
            _ssh_host = MyTest.MyTestOptions.Default.SSHHost;
            _ssh_port = MyTest.MyTestOptions.Default.SSHPort;
            _ssh_username = MyTest.MyTestOptions.Default.SSHUser;
            _ssh_password = MyTest.MyTestOptions.Default.SSHPassword;
            CASSANDRA_VERSION = "-v " + version;
#else
            var version = Cassandra.Properties.Settings.Default.CASSANDRA_VERSION;
            IP_PREFIX = Cassandra.Properties.Settings.Default.IP_PREFIX;
            if (string.IsNullOrEmpty(IP_PREFIX))
                IP_PREFIX = "127.0.0.";

            if (new Regex(CASSANDRA_VERSION_REGEXP).IsMatch(version))
            {
                CASSANDRA_DIR = null;
                CASSANDRA_VERSION = "-v " + version;
            }
            else
            {
                CASSANDRA_DIR = Cassandra.Properties.Settings.Default.CASSANDRA_DIR;
                CASSANDRA_VERSION = null;
            }

            _ssh_host = Cassandra.Properties.Settings.Default.SSH_HOST;
            _ssh_port = Cassandra.Properties.Settings.Default.SSH_PORT;
            _ssh_username = Cassandra.Properties.Settings.Default.SSH_USERNAME;
            _ssh_password = Cassandra.Properties.Settings.Default.SSH_PASSWORD;
#endif


        }

        static string _ssh_host;
        static int _ssh_port;
        static string _ssh_username;
        static string _ssh_password;

        private readonly DirectoryInfo _ccmDir;
        private Renci.SshNet.SshClient _ssh_client;
        private Renci.SshNet.ShellStream _ssh_shellStream;

        private CCMBridge()
        {
            _ccmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            _ssh_client = new Renci.SshNet.SshClient(_ssh_host, _ssh_port, _ssh_username, _ssh_password);
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
            bridge.ExecuteCCM(string.Format("Create {0} -b -i {1} {2}", name, IP_PREFIX, CASSANDRA_VERSION));
            return bridge;
        }

        public static CCMBridge Create(string name, int nbNodes)
        {
            CCMBridge bridge = new CCMBridge();
            bridge.ExecuteCCM(string.Format("Create {0} -n {1} -s -i {2} -b {3}", name, nbNodes, IP_PREFIX, CASSANDRA_VERSION));
            return bridge;
        }

        public static CCMBridge Create(string name, int nbNodesDC1, int nbNodesDC2)
        {
            CCMBridge bridge = new CCMBridge();
            bridge.ExecuteCCM(string.Format("Create {0} -n {1}:{2} -s -i {3} -b {4}", name, nbNodesDC1, nbNodesDC2, IP_PREFIX, CASSANDRA_VERSION));
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
            if (dc == null)
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b", n, IP_PREFIX, n, 7000 + 100 * n));
            else
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b -d {4}", n, IP_PREFIX, n, 7000 + 100 * n, dc));
            ExecuteCCM(string.Format("node{0} start", n));
        }

        public void DecommissionNode(int n)
        {
            ExecuteCCM(string.Format("node{0} decommission", n));
        }

        private void ExecuteCCM(string args)
        {
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
                if (outp.ToString().Contains("[Errno 17]"))
                {
                    ExecuteCCMAndPrint("remove test");
                    ExecuteCCM(args);
                    return;
                }
                var lines = outp.ToString().Split('\n');
                for (int i = 0; i < lines.Length; i++)
                    Trace.TraceError("err>" + lines[i].Trim());
                throw new InvalidOperationException();
            }
        }

        private void ExecuteCCMAndPrint(string args)
        {
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
        }

        // One cluster for the whole test class
        public abstract class PerClassSingleNodeCluster
        {

            protected static CCMBridge cassandraCluster;
            private static bool erroredOut;
            private static bool schemaCreated;

            protected static Cluster cluster;
            protected static Session session;

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
                    cluster = Cluster.Builder().AddContactPoints(IP_PREFIX + "1").Build();
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
                    catch (AlreadyExistsException e)
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
                        catch (AlreadyExistsException e)
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

        public class CCMCluster
        {

            public readonly Cluster Cluster;
            public readonly Session Session;

            public readonly CCMBridge CassandraCluster;

            private bool erroredOut;

            public static CCMCluster Create(int nbNodes, Builder builder)
            {
                if (nbNodes == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodes), builder);
            }

            public static CCMCluster Create(int nbNodesDC1, int nbNodesDC2, Builder builder)
            {
                if (nbNodesDC1 == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodesDC1, nbNodesDC2), builder);
            }

            private CCMCluster(CCMBridge cassandraCluster, Builder builder)
            {
                this.CassandraCluster = cassandraCluster;
                try
                {
                    this.Cluster = builder.AddContactPoints(IP_PREFIX + "1").Build();
                    this.Session = Cluster.Connect();

                }
                catch (NoHostAvailableException e)
                {
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

                if (CassandraCluster == null)
                {
                    Trace.TraceError("No cluster to discard");
                }
                else if (erroredOut)
                {
                    CassandraCluster.Stop();
                    Trace.TraceInformation("Error during tests, kept C* logs in " + CassandraCluster._ccmDir);
                }
                else
                {
                    CassandraCluster.Remove();
                    CassandraCluster._ccmDir.Delete();
                }
            }
        }


    }
}