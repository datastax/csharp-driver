using System;
using System.IO;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Collections.Generic;
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
            var version = Cassandra.Properties.Settings.Default.CASSANDRA_VERSION;

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
            IP_PREFIX = Cassandra.Properties.Settings.Default.IP_PREFIX;
            if (string.IsNullOrEmpty(IP_PREFIX))
                IP_PREFIX = "127.0.0.";

            host = Cassandra.Properties.Settings.Default.SSH_HOST;
            port = Cassandra.Properties.Settings.Default.SSH_PORT;
            username = Cassandra.Properties.Settings.Default.SSH_USERNAME;
            password = Cassandra.Properties.Settings.Default.SSH_PASSWORD;

        }

        static string host;
        static int port;
        static string username;
        static string password;

        private readonly DirectoryInfo ccmDir;
        private Renci.SshNet.SshClient client;
        private Renci.SshNet.ShellStream shellStream;

        private CCMBridge()
        {
            this.ccmDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            client = new Renci.SshNet.SshClient(host, port, username, password);
            client.Connect();
            shellStream = client.CreateShellStream("CCM", 80, 60, 100, 100, 1000);
            while (true)
            {
                var txt = shellStream.Read();
                Console.Write(txt);
                if (txt.Contains("$"))
                    break;
            }
        }

        public void Dispose()
        {
            if (client != null)
            {
                client.Disconnect();
                client = null;
            }
        }

        ~CCMBridge()
        {
            if (client != null)
                client.Disconnect();
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

        public void start()
        {
            ExecuteCCM("start");
        }

        public void stop()
        {
            ExecuteCCM("stop");
        }

        public void forceStop()
        {
            ExecuteCCM("stop --not-gently");
        }

        public void start(int n)
        {
            ExecuteCCM(string.Format("node{0} start", n));
        }

        public void stop(int n)
        {
            ExecuteCCM(string.Format("node{0} stop", n));
        }

        public void forceStop(int n)
        {
            ExecuteCCM(string.Format("node{0} stop --not-gently", n));
        }

        public void remove()
        {
            stop();
            ExecuteCCM(string.Format("remove"));
        }

        public void ring(int n)
        {
            ExecuteCCMAndPrint(string.Format("node{0} ring", n));
        }

        public void bootstrapNode(int n)
        {
            bootstrapNode(n, null);
        }

        public void bootstrapNode(int n, string dc)
        {
            if (dc == null)
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b", n, IP_PREFIX, n, 7000 + 100 * n));
            else
                ExecuteCCM(string.Format("add node{0} -i {1}{2} -j {3} -b -d {4}", n, IP_PREFIX, n, 7000 + 100 * n, dc));
            ExecuteCCM(string.Format("node%d start", n));
        }

        public void decommissionNode(int n)
        {
            ExecuteCCM(string.Format("node{0} decommission", n));
        }

        private void ExecuteCCM(string args)
        {
            shellStream.WriteLine("ccm " + args);
            while (true)
            {
                var txt = shellStream.Read();
                if (txt.Contains("$"))
                    break;
            }
        }

        private void ExecuteCCMAndPrint(string args)
        {
            shellStream.WriteLine("ccm " + args);
            while (true)
            {
                var txt = shellStream.Read();
                Console.Write(txt);
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
            protected static Session session;

            protected abstract ICollection<string> getTableDefinitions();

            public void errorOut()
            {
                erroredOut = true;
            }

            public static void createCluster()
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

            public static void discardCluster()
            {
                if (cluster != null)
                    cluster.Shutdown();

                if (cassandraCluster == null)
                {
                    Trace.TraceError("No cluster to discard");
                }
                else if (erroredOut)
                {
                    cassandraCluster.stop();
                    Trace.TraceInformation("Error during tests, kept C* logs in " + cassandraCluster.ccmDir);
                }
                else
                {
                    cassandraCluster.remove();
                    cassandraCluster.ccmDir.Delete();
                }
            }

            public void beforeClass()
            {
                createCluster();
                maybeCreateSchema();
            }

            public void maybeCreateSchema()
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

                    foreach (string tableDef in getTableDefinitions())
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

            public readonly Cluster cluster;
            public readonly Session session;

            public readonly CCMBridge cassandraCluster;

            private bool erroredOut;

            public static CCMCluster create(int nbNodes, Builder builder)
            {
                if (nbNodes == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodes), builder);
            }

            public static CCMCluster create(int nbNodesDC1, int nbNodesDC2, Builder builder)
            {
                if (nbNodesDC1 == 0)
                    throw new ArgumentException();

                return new CCMCluster(CCMBridge.Create("test", nbNodesDC1, nbNodesDC2), builder);
            }

            private CCMCluster(CCMBridge cassandraCluster, Builder builder)
            {
                this.cassandraCluster = cassandraCluster;
                try
                {
                    this.cluster = builder.AddContactPoints(IP_PREFIX + "1").Build();
                    this.session = cluster.Connect();

                }
                catch (NoHostAvailableException e)
                {
                    foreach (var entry in e.Errors)
                        Trace.TraceError("Error connecting to " + entry.Key + ": " + entry.Value);
                    throw new InvalidOperationException(null, e);
                }
            }

            public void errorOut()
            {
                erroredOut = true;
            }

            public void discard()
            {
                if (cluster != null)
                    cluster.Shutdown();

                if (cassandraCluster == null)
                {
                    Trace.TraceError("No cluster to discard");
                }
                else if (erroredOut)
                {
                    cassandraCluster.stop();
                    Trace.TraceInformation("Error during tests, kept C* logs in " + cassandraCluster.ccmDir);
                }
                else
                {
                    cassandraCluster.remove();
                    cassandraCluster.ccmDir.Delete();
                }
            }
        }


    }
}