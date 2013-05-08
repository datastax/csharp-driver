using System;
using System.Net;
using System.Threading;
namespace Cassandra
{

    /// <summary>
		///  A number of static fields/methods handy for tests.
		/// </summary>

public static class TestUtils {

    private static readonly Logger logger = new Logger(typeof(TestUtils));
    public static readonly string CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {1} }}";
    public static readonly string CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";

    public static readonly string SIMPLE_KEYSPACE = "ks";

    public static readonly string CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";

    public static readonly string INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
    public static readonly string SELECT_ALL_FORMAT = "SELECT * FROM {0}";
    public static readonly string SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";

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

    private static void waitFor(string node, Cluster cluster, int maxTry, bool waitForDead, bool waitForOut) {
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
        try {
             address = IPAddress.Parse(node);
        } catch (Exception e) {
            // That's a problem but that's not *our* problem
            return;
        }

        Metadata metadata = cluster.Metadata;
        for (int i = 0; i < maxTry; ++i) {
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
            try { Thread.Sleep(1000); } catch (Exception e) {}
        }

        foreach(Host host in metadata.AllHosts()) 
        {
            if (host.Address.Equals(address)) {
                if (testHost(host, waitForDead)) {
                    return;
                } else {
                    // logging it because this give use the timestamp of when this happens
                    logger.Info(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                    throw new InvalidOperationException(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                }
            }
        }

        if (waitForOut){
            return;
        } else {
            logger.Info(node + " is not part of the cluster after " + maxTry + "s");
            throw new InvalidOperationException(node + " is not part of the cluster after " + maxTry + "s");
        }
    }

    private static bool testHost(Host host, bool testForDown)
    {
        return testForDown ? !host.IsConsiderablyUp: host.IsConsiderablyUp;
    }
}
}	// end namespace