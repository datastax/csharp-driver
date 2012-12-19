using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Net;

namespace Cassandra.Native
{
    internal class ControlConnection
    {
        CassandraSession session;
        CassandraSession owner;
        CassandraClusterHost current = null;
        public ControlConnection(CassandraSession owner, IEnumerable<IPEndPoint> clusterEndpoints, string keyspace, CassandraCompressionType compression = CassandraCompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, Policies policies = null, AuthInfoProvider credentialsDelegate = null, PoolingOptions poolingOptions = null, bool noBufferingIfPossible = false)
        {
            this.owner = owner;
            this.reconnectionTimer = new Timer(reconnectionClb, null, Timeout.Infinite, Timeout.Infinite);
            session = new CassandraSession(clusterEndpoints, keyspace, compression, abortTimeout, policies, credentialsDelegate, poolingOptions, noBufferingIfPossible, true);
            go();
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
                        theExc = new Exception("CQL Error [" + (ret as OutputError).CassandraErrorType.ToString() + "] " + (ret as OutputError).Message);
                    else
                        theExc = new CassandraClientProtocolViolationException("Expected Error on Output");
                }
            }

            if (theExc != null)
                throw new CassandraConnectionException("Register event", theExc);

        }

        void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e.CassandraEventType == CassandraEventType.StatusChange || e.CassandraEventType == CassandraEventType.TopologyChange)
            {
                if (e.Message == "UP" || e.Message == "NEW_NODE")
                {
                    owner.OnAddHost(e.IPEndPoint);
                    session.OnAddHost(e.IPEndPoint);
                    checkConnectionUp(e.IPEndPoint);
                    return;
                }
                else if (e.Message == "REMOVED_NODE")
                {
                    owner.OnRemovedHost(e.IPEndPoint);
                    session.OnRemovedHost(e.IPEndPoint);
                    checkConnectionDown(e.IPEndPoint);
                    return;
                }
                else if (e.Message == "DOWN")
                {
                    owner.OnDownHost(e.IPEndPoint);
                    session.OnDownHost(e.IPEndPoint);
                    checkConnectionDown(e.IPEndPoint);
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
            throw new CassandraClientProtocolViolationException("Unknown Event");
        }

        internal void ownerHostIsDown(IPEndPoint endpoint)
        {
            session.OnDownHost(endpoint);
            checkConnectionDown(endpoint);
        }

        internal void ownerHostBringUpIfDown(IPEndPoint endpoint)
        {
            session.OnAddHost(endpoint);
            checkConnectionUp(endpoint);
        }

        bool isDiconnected = false;
        Timer reconnectionTimer;

        void reconnectionClb(object state)
        {
            go();
        }
        ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(2 * 1000, 5 * 60 * 1000);
        ReconnectionSchedule reconnectionSchedule = null;

        void go()
        {
            try
            {
                reconnectionTimer.Change(Timeout.Infinite, Timeout.Infinite);
                setupEventListeners(session.connect(null, ref current));
            }
            catch (CassandraNoHostAvaliableException)
            {
                isDiconnected = true;
                reconnectionTimer.Change(reconnectionSchedule.NextDelayMs(), Timeout.Infinite);
            }
        }

        void checkConnectionDown(IPEndPoint endpoint)
        {
            if (current.Address == endpoint)
            {
                reconnectionSchedule = reconnectionPolicy.NewSchedule();
                go();
            }
        }
        void checkConnectionUp(IPEndPoint endpoint)
        {
            if (isDiconnected)
            {
                reconnectionSchedule = reconnectionPolicy.NewSchedule();
                go();
            }
        }
    }
}
