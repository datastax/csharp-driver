using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net;

namespace Cassandra.Native
{
    internal class ControlConnection
    {
        Session session;
        Session owner;
        Host current = null;
        public ControlConnection(Session owner, IEnumerable<IPAddress> clusterEndpoints, int port, string keyspace, CompressionType compression = CompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies policies = null, AuthInfoProvider credentialsDelegate = null, PoolingOptions poolingOptions = null, bool noBufferingIfPossible = false)
        {
            this.owner = owner;
            this.reconnectionTimer = new Timer(reconnectionClb, null, Timeout.Infinite, Timeout.Infinite);
            session = new Session(clusterEndpoints, port, keyspace, compression, abortTimeout, policies, credentialsDelegate, poolingOptions, noBufferingIfPossible, owner.Hosts);
            metadata = new ClusterMetadata(owner.Hosts);
            go(true);
        }

        private void setupEventListeners(CassandraConnection nconn)
        {
            Exception theExc = null;

            nconn.CassandraEvent += new CassandraEventHandler(conn_CassandraEvent);
            using (var ret = nconn.RegisterForCassandraEvent(
                CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange))
            {
                if (!(ret is OutputVoid))
                {
                    if (ret is OutputError)
                        theExc = (ret as OutputError).CreateException();
                    else
                        theExc = new DriverInternalError("Expected Error on Output");
                }
            }

            if (theExc != null)
                throw theExc;
        }

        void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e.CassandraEventType == CassandraEventType.StatusChange || e.CassandraEventType == CassandraEventType.TopologyChange)
            {
                if (e.Message == "UP" || e.Message == "NEW_NODE")
                {
                    owner.OnAddHost(e.IPAddress);
                    session.OnAddHost(e.IPAddress);
                    checkConnectionUp(e.IPAddress);
                    return;
                }
                else if (e.Message == "REMOVED_NODE")
                {
                    owner.OnRemovedHost(e.IPAddress);
                    session.OnRemovedHost(e.IPAddress);
                    checkConnectionDown(e.IPAddress);
                    return;
                }
                else if (e.Message == "DOWN")
                {
                    owner.OnDownHost(e.IPAddress);
                    session.OnDownHost(e.IPAddress);
                    checkConnectionDown(e.IPAddress);
                    return;
                }
            }

            if (e.CassandraEventType == CassandraEventType.SchemaChange)
            {
                if (e.Message.StartsWith("CREATED") || e.Message.StartsWith("UPDATED") || e.Message.StartsWith("DROPPED"))
                {
                }
                return;
            }
            throw new DriverInternalError("Unknown Event");
        }

        internal void ownerHostIsDown(IPAddress endpoint)
        {
            session.OnDownHost(endpoint);
            checkConnectionDown(endpoint);
        }

        internal void ownerHostBringUpIfDown(IPAddress endpoint)
        {
            session.OnAddHost(endpoint);
            checkConnectionUp(endpoint);
        }

        bool isDiconnected = false;
        Timer reconnectionTimer;

        void reconnectionClb(object state)
        {
            go(true);
        }
        ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000);
        ReconnectionSchedule reconnectionSchedule = null;

        CassandraConnection connection = null;
        internal        ClusterMetadata metadata;


        void go(bool refresh)
        {
            try
            {
                reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                connection =session.connect(null, ref current);
                setupEventListeners(connection);
                if(refresh)
                    refreshNodeListAndTokenMap(connection);
            }
            catch (NoHostAvailableException)
            {
                isDiconnected = true;
                reconnectionTimer.Change(reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    isDiconnected = true;
                    reconnectionTimer.Change(reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                else
                    throw;
            }
        }

        void checkConnectionDown(IPAddress endpoint)
        {
            if (current.Address == endpoint)
            {
                reconnectionSchedule = reconnectionPolicy.NewSchedule();
                go(false);
            }
        }
        void checkConnectionUp(IPAddress endpoint)
        {
            if (isDiconnected)
            {
                reconnectionSchedule = reconnectionPolicy.NewSchedule();
                go(true);
            }
        }


        // schema
        private static readonly long MAX_SCHEMA_AGREEMENT_WAIT_MS = 10000;

        private static readonly String SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces";
        private static readonly String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
        private static readonly String SELECT_COLUMNS = "SELECT * FROM system.schema_columns";

        private static readonly String SELECT_PEERS = "SELECT peer, data_center, rack, tokens FROM system.peers";
        private static readonly String SELECT_LOCAL = "SELECT cluster_name, data_center, rack, tokens, partitioner FROM system.local WHERE key='local'";

        private static readonly String SELECT_SCHEMA_PEERS = "SELECT peer, schema_version FROM system.peers";
        private static readonly String SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'";

        private void refreshNodeListAndTokenMap(CassandraConnection connection)
        {
            // Make sure we're up to date on nodes and tokens

            Dictionary<IPAddress, DictSet<string>> tokenMap = new Dictionary<IPAddress, DictSet<string>>();
            string partitioner = null;

            using (var rowset = session.Query(SELECT_LOCAL, ConsistencyLevel.DEFAULT))
            {
                // Update cluster name, DC and rack for the one node we are connected to
                foreach (var localRow in rowset.GetRows())
                {
                    var clusterName = localRow.GetValue<string>("cluster_name");
                    if (clusterName != null)
                        metadata.clusterName = clusterName;

                    var host = metadata.GetHost(connection.getAdress());
                    // In theory host can't be null. However there is no point in risking a NPE in case we
                    // have a race between a node removal and this.
                    if (host != null)
                        host.SetLocationInfo(localRow.GetValue<string>("data_center"), localRow.GetValue<string>("rack"));

                    partitioner = localRow.GetValue<string>("partitioner");
                    var tokens = localRow.GetValue<IList<string>>("tokens");
                    if (partitioner != null && tokens.Count > 0)
                    {
                        if (!tokenMap.ContainsKey(host.Address))
                            tokenMap.Add(host.Address, new DictSet<string>());
                        tokenMap[host.Address].AddRange(tokens);
                    }
                    break;
                }
            }

            List<IPAddress> foundHosts = new List<IPAddress>();
            List<string> dcs = new List<string>();
            List<string> racks = new List<string>();
            List<DictSet<string>> allTokens = new List<DictSet<string>>();

            using (var rowset = session.Query(SELECT_PEERS, ConsistencyLevel.DEFAULT))
            {
                foreach (var row in rowset.GetRows())
                {
                    var hstip = row.GetValue<IPAddress>("peer");
                    if (hstip != null)
                    {
                        foundHosts.Add(hstip);
                        dcs.Add(row.GetValue<string>("data_center"));
                        racks.Add(row.GetValue<string>("rack"));
                        allTokens.Add(new DictSet<string>(row.GetValue<IEnumerable<string>>("tokens")));
                    }
                }
            }

            for (int i = 0; i < foundHosts.Count; i++)
            {
                var host = metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = metadata.AddHost(foundHosts[i], owner.Policies.ReconnectionPolicy);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && !allTokens[i].IsEmpty)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            DictSet<IPAddress> foundHostsSet = new DictSet<IPAddress>(foundHosts);
            foreach (var host in metadata.AllHosts())
                if (!host.Equals(connection.getAdress()) && !foundHostsSet.Contains(host))
                    metadata.RemoveHost(host);

            if (partitioner != null)
                metadata.rebuildTokenMap(partitioner, tokenMap);
        }

        //static boolean waitForSchemaAgreement(Connection connection, Metadata metadata) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        //    long start = System.currentTimeMillis();
        //    long elapsed = 0;
        //    while (elapsed < MAX_SCHEMA_AGREEMENT_WAIT_MS) {
        //        ResultSetFuture peersFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_PEERS, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
        //        ResultSetFuture localFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_LOCAL, ConsistencyLevel.DEFAULT_CASSANDRA_CL));
        //        connection.write(peersFuture.callback);
        //        connection.write(localFuture.callback);

        //        Set<UUID> versions = new HashSet<UUID>();

        //        Row localRow = localFuture.get().fetchOne();
        //        if (localRow != null && !localRow.isNull("schema_version"))
        //            versions.add(localRow.getUUID("schema_version"));

        //        for (Row row : peersFuture.get()) {
        //            if (row.isNull("peer") || row.isNull("schema_version"))
        //                continue;

        //            Host peer = metadata.getHost(row.getInet("peer"));
        //            if (peer != null && peer.getMonitor().isUp())
        //                versions.add(row.getUUID("schema_version"));
        //        }

        //        if (versions.size() <= 1)
        //            return true;

        //        // let's not flood the node too much
        //        try { Thread.sleep(200); } catch (InterruptedException e) {};

        //        elapsed = System.currentTimeMillis() - start;
        //    }

        //    return false;
        //}
    }
}
