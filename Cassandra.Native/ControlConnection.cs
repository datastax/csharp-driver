using System;
using System.Collections.Generic;
using System.Threading;
using System.Net;

namespace Cassandra
{
    internal class ControlConnection
    {
        readonly Session _session;
        readonly Session _owner;
        IEnumerator<Host> _hostsIter = null;
        public ControlConnection(Session owner, IEnumerable<IPAddress> clusterEndpoints, int port, string keyspace, CompressionType compression = CompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies policies = null, IAuthInfoProvider credentialsDelegate = null, PoolingOptions poolingOptions = null, bool noBufferingIfPossible = false)
        {
            this._owner = owner;
            this._reconnectionTimer = new Timer(ReconnectionClb, null, Timeout.Infinite, Timeout.Infinite);
            _session = new Session(clusterEndpoints, port, keyspace, compression, abortTimeout, policies, credentialsDelegate, poolingOptions, noBufferingIfPossible, owner.Hosts);
            Metadata = new ClusterMetadata(owner.Hosts);
            go(true);
        }

        private void SetupEventListeners(CassandraConnection nconn)
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
                    _owner.OnAddHost(e.IPAddress);
                    _session.OnAddHost(e.IPAddress);
                    CheckConnectionUp(e.IPAddress);
                    return;
                }
                else if (e.Message == "REMOVED_NODE")
                {
                    _owner.OnRemovedHost(e.IPAddress);
                    _session.OnRemovedHost(e.IPAddress);
                    CheckConnectionDown(e.IPAddress);
                    return;
                }
                else if (e.Message == "DOWN")
                {
                    _owner.OnDownHost(e.IPAddress);
                    _session.OnDownHost(e.IPAddress);
                    CheckConnectionDown(e.IPAddress);
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

        internal void OwnerHostIsDown(IPAddress endpoint)
        {
            _session.OnDownHost(endpoint);
            CheckConnectionDown(endpoint);
        }

        internal void OwnerHostBringUpIfDown(IPAddress endpoint)
        {
            _session.OnAddHost(endpoint);
            CheckConnectionUp(endpoint);
        }

        bool _isDiconnected = false;
        readonly Timer _reconnectionTimer;

        void ReconnectionClb(object state)
        {
            go(true);
        }

        readonly IReconnectionPolicy _reconnectionPolicy = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000);
        IReconnectionSchedule _reconnectionSchedule = null;

        CassandraConnection _connection = null;
        internal ClusterMetadata Metadata;


        void go(bool refresh)
        {
            try
            {
                if (_hostsIter == null)
                    _hostsIter = _owner.Policies.LoadBalancingPolicy.NewQueryPlan(null).GetEnumerator();

                if (!_hostsIter.MoveNext())
                {
                    _isDiconnected = true;
                    _hostsIter = null;
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                else
                {
                    _reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    _connection = _session.Connect(null, _hostsIter);
                    SetupEventListeners(_connection);
                    if (refresh)
                        refreshNodeListAndTokenMap(_connection);
                }
            }
            catch (NoHostAvailableException)
            {
                _isDiconnected = true;
                _hostsIter = null;
                _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
            }
            catch (Exception ex)
            {
                if (CassandraConnection.IsStreamRelatedException(ex))
                {
                    _isDiconnected = true;
                    _hostsIter = null;
                    _reconnectionTimer.Change(_reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
                }
                else
                    throw;
            }
        }

        void CheckConnectionDown(IPAddress endpoint)
        {
            if (_hostsIter!=null && _hostsIter.Current.Address == endpoint)
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
                go(false);
            }
        }
        void CheckConnectionUp(IPAddress endpoint)
        {
            if (_isDiconnected)
            {
                _reconnectionSchedule = _reconnectionPolicy.NewSchedule();
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

            using (var rowset = _session.Query(SELECT_LOCAL, ConsistencyLevel.Default))
            {
                // Update cluster name, DC and rack for the one node we are connected to
                foreach (var localRow in rowset.GetRows())
                {
                    var clusterName = localRow.GetValue<string>("cluster_name");
                    if (clusterName != null)
                        Metadata.ClusterName = clusterName;

                    var host = Metadata.GetHost(connection.GetHostAdress());
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

            using (var rowset = _session.Query(SELECT_PEERS, ConsistencyLevel.Default))
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
                var host = Metadata.GetHost(foundHosts[i]);
                if (host == null)
                {
                    // We don't know that node, add it.
                    host = Metadata.AddHost(foundHosts[i], _owner.Policies.ReconnectionPolicy);
                }
                host.SetLocationInfo(dcs[i], racks[i]);

                if (partitioner != null && !allTokens[i].IsEmpty)
                    tokenMap.Add(host.Address, allTokens[i]);
            }

            // Removes all those that seems to have been removed (since we lost the control connection)
            DictSet<IPAddress> foundHostsSet = new DictSet<IPAddress>(foundHosts);
            foreach (var host in Metadata.AllHosts())
                if (!host.Equals(connection.GetHostAdress()) && !foundHostsSet.Contains(host))
                    Metadata.RemoveHost(host);

            if (partitioner != null)
                Metadata.RebuildTokenMap(partitioner, tokenMap);
        }

        //static boolean waitForSchemaAgreement(Connection connection, Metadata metadata) throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

        //    long start = System.currentTimeMillis();
        //    long elapsed = 0;
        //    while (elapsed < MAX_SCHEMA_AGREEMENT_WAIT_MS) {
        //        ResultSetFuture peersFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_PEERS, Consistency.DEFAULT_CASSANDRA_CL));
        //        ResultSetFuture localFuture = new ResultSetFuture(null, new QueryMessage(SELECT_SCHEMA_LOCAL, Consistency.DEFAULT_CASSANDRA_CL));
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
