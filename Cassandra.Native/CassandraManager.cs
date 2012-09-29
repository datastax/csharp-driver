using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace Cassandra.Native
{
    public class CassandraManager : IDisposable
    {
        CredentialsDelegate credentialsDelegate;

        CassandraCompressionType Compression;
        BufferingMode Buffering;
        int abortTimeout;

        class ClientsPool : IDisposable
        {
            List<CassandraClient> clients = new List<CassandraClient>();

            public ClientsPool(int initialCount)
            {
                lock (clients)
                {
                    for (int i = 0; i < initialCount; i++)
                        clients.Add(new CassandraClient());
                }
            }

            public void Add()
            {
                lock (clients)
                    clients.Add(new CassandraClient());
            }

            public void Dispose()
            {
                lock (clients)
                    foreach (var client in clients)
                        client.Dispose();
            }

            public CassandraClient GetOne()
            {
                lock (clients)
                    return clients[StaticRandom.Instance.Next(clients.Count)];
            }
        }

        ClientsPool clientPool;

        List<string> loadedClusterEndpoints;
        List<string> upClusterEndpoints;
        Queue<WeakReference<CassandraConnection>> connectionQueue = new Queue<WeakReference<CassandraConnection>>();
        int createdConnections = 0;
        int maxConnectionsInPool = int.MaxValue;

        public CassandraManager(IEnumerable<IPEndPoint> clusterEndpoints, CassandraCompressionType compression = CassandraCompressionType.NoCompression, BufferingMode buffering = BufferingMode.FrameBuffering,
            int abortTimeout = Timeout.Infinite, CredentialsDelegate credentialsDelegate = null, int dispatchThreadCount = 1, int maxConnectionsInPool = int.MaxValue)
        {
            this.maxConnectionsInPool = maxConnectionsInPool;

            this.loadedClusterEndpoints = new List<string>();
            foreach (var ep in clusterEndpoints)
                loadedClusterEndpoints.Add(ep.ToString());

            this.upClusterEndpoints = new List<string>(loadedClusterEndpoints);

            this.Compression = compression;
            this.Buffering = buffering;
            this.abortTimeout = abortTimeout;

            this.credentialsDelegate = credentialsDelegate;
            this.clientPool = new ClientsPool(dispatchThreadCount);
        }
        //public CassandraManagedConnection Connect(CassandraCompressionType compression = CassandraCompressionType.NoCompression, BufferingMode mode = BufferingMode.FrameBuffering, int abortTimeout = Timeout.Infinite)
        
                
        public CassandraManagedConnection Connect()
        {
            checkDisposed();

            lock (connectionQueue)
            {
            retry:
                if (connectionQueue.Count > 0)
                {
                    var conn = connectionQueue.Dequeue();
                    if (!conn.IsAlive)
                        goto retry;
                    return new CassandraManagedConnection(this, conn.Value);
                }
                if (createdConnections >= maxConnectionsInPool - 1)
                    throw new InvalidOperationException();
            }

            IPEndPoint endPoint = null;
            lock (upClusterEndpoints)
                endPoint = IPEndPointParser.ParseEndpoint(upClusterEndpoints[StaticRandom.Instance.Next(upClusterEndpoints.Count)]);

            Exception theExc = null;
            CassandraConnection nconn = null;

            try
            {
                nconn = clientPool.GetOne().Connect(endPoint, credentialsDelegate, this.Compression, this.Buffering, this.abortTimeout);

                var options = nconn.ExecuteOptions();

                nconn.CassandraEvent += new CassandraEventHandler(conn_CassandraEvent);
                using (var ret = nconn.RegisterForCassandraEvent(
                    CassandraEventType.TopologyChange | CassandraEventType.StatusChange))
                {
                    if (!(ret is OutputVoid))
                    {
                        if (ret is OutputError)
                            theExc = new Exception("CQL Error [" + (ret as OutputError).CassandraErrorType.ToString() + "] " + (ret as OutputError).Message);
                        else
                            theExc = new InvalidOperationException();
                    }
                }

                if (theExc != null)
                    throw new CassandraConnectionException("Register event", theExc);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message, "CassandraManager.Connect");
                if (nconn != null)
                    nconn.Dispose();
                throw new CassandraConnectionException("Cannot connect", ex);
            }

            lock (connectionQueue)
            {
                createdConnections++;
                return new CassandraManagedConnection(this, nconn);
            }
        }

        internal void ReleaseConnection(CassandraConnection conn)
        {
            lock (connectionQueue)
            {
                if (conn.IsHealthy)
                    connectionQueue.Enqueue(new WeakReference<CassandraConnection>(conn));
                else
                {
                    createdConnections--;
                    conn.Dispose();
                }
            }
        }

        void conn_CassandraEvent(object sender, CassandraEventArgs e)
        {
            if (e.CassandraEventType == CassandraEventType.StatusChange || e.CassandraEventType == CassandraEventType.TopologyChange)
            {
                if (e.Message == "UP" || e.Message == "NEW_NODE")
                {
                    lock(upClusterEndpoints)
                        if (!loadedClusterEndpoints.Contains(e.IPEndPoint.ToString()))
                            upClusterEndpoints.Add(e.IPEndPoint.ToString());
                    return;
                }
                else if (e.Message == "DOWN" ||e.Message == "REMOVED_NODE")
                {
                    lock (upClusterEndpoints)
                        if (!upClusterEndpoints.Contains(e.IPEndPoint.ToString()))
                            upClusterEndpoints.Remove(e.IPEndPoint.ToString());
                    return;
                }
            }
            throw new InvalidOperationException();
        }

        Wrapper<bool> alreadyDisposed = new Wrapper<bool>(false);

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraManager");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (alreadyDisposed.Value)
                    return;
                alreadyDisposed.Value = true;
                lock(connectionQueue)
                {
                    if (createdConnections != connectionQueue.Count)
                    {
                        throw new InvalidOperationException();
                    }
                    while (connectionQueue.Count > 0)
                    {
                        var conn = connectionQueue.Dequeue();
                        if (conn.IsAlive)
                            conn.Value.Dispose();
                    }
                }
                clientPool.Dispose();
            }
        }

        ~CassandraManager()
        {
            Dispose();
        }
    }
}
