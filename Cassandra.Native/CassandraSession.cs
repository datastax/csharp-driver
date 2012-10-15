using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace Cassandra.Native
{
    public class CassandraSession : IDisposable
    {
        CredentialsDelegate credentialsDelegate;

        CassandraCompressionType compression;
        int abortTimeout;

        List<string> loadedClusterEndpoints;
        List<string> upClusterEndpoints;
        Queue<WeakReference<CassandraConnection>> connectionQueue = new Queue<WeakReference<CassandraConnection>>();
        int createdConnections = 0;
        int maxConnectionsInPool = int.MaxValue;
        string keyspace = "";

        CassandraConnection eventRaisingConnection = null;

        public CassandraSession(IEnumerable<IPEndPoint> clusterEndpoints, string keyspace, CassandraCompressionType compression = CassandraCompressionType.NoCompression,
            int abortTimeout = Timeout.Infinite, CredentialsDelegate credentialsDelegate = null, int maxConnectionsInPool = int.MaxValue)
        {
            this.maxConnectionsInPool = maxConnectionsInPool;

            this.loadedClusterEndpoints = new List<string>();
            foreach (var ep in clusterEndpoints)
                loadedClusterEndpoints.Add(ep.ToString());

            this.upClusterEndpoints = new List<string>(loadedClusterEndpoints);

            this.compression = compression;
            this.abortTimeout = abortTimeout;

            this.credentialsDelegate = credentialsDelegate;
            this.keyspace = keyspace;
            var conn = connect();
            try
            {
                setupEventListeners(conn);
            }
            finally
            {
                releaseConnection(conn);
            }
        }

        private void setupEventListeners(CassandraConnection nconn)
        {
            Exception theExc = null;

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

            eventRaisingConnection = nconn;
        }

        private CassandraConnection connect()
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
                    return conn.Value;
                }
                if (createdConnections >= maxConnectionsInPool - 1)
                    throw new InvalidOperationException();
            }

            IPEndPoint endPoint = null;
            lock (upClusterEndpoints)
                endPoint = IPEndPointParser.ParseEndpoint(upClusterEndpoints[StaticRandom.Instance.Next(upClusterEndpoints.Count)]);

            CassandraConnection nconn = null;

            try
            {
                nconn = new CassandraConnection(endPoint, credentialsDelegate, this.compression, this.abortTimeout);

                var options = nconn.ExecuteOptions();

                if (!string.IsNullOrEmpty(keyspace))
                {
                    var outp = nconn.Query("USE \"" + keyspace.Replace("\"", "\"\""));
                }

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
                return nconn;
            }
        }

        private void releaseConnection(CassandraConnection conn)
        {
            lock (connectionQueue)
            {
                if (conn.IsHealthy)
                    connectionQueue.Enqueue(new WeakReference<CassandraConnection>(conn));
                else
                {
                    if (eventRaisingConnection == conn)
                    {
                        var nconn = connect();
                        try
                        {
                            setupEventListeners(nconn);
                        }
                        finally
                        {
                            releaseConnection(nconn);
                        }
                    }
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
                    lock (upClusterEndpoints)
                        if (!loadedClusterEndpoints.Contains(e.IPEndPoint.ToString()))
                            upClusterEndpoints.Add(e.IPEndPoint.ToString());
                    return;
                }
                else if (e.Message == "DOWN" || e.Message == "REMOVED_NODE")
                {
                    lock (upClusterEndpoints)
                        if (!upClusterEndpoints.Contains(e.IPEndPoint.ToString()))
                            upClusterEndpoints.Remove(e.IPEndPoint.ToString());
                    return;
                }
            }
            throw new InvalidOperationException();
        }

        Guarded<bool> alreadyDisposed = new Guarded<bool>(false);

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraSession");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (alreadyDisposed.Value)
                    return;
                alreadyDisposed.Value = true;
                lock (connectionQueue)
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
            }
        }

        ~CassandraSession()
        {
            Dispose();
        }

        class ConnectionWrapper
        {
            public CassandraConnection connection;
            public bool delayedRelease;
        }

        private void processNonQuery(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputVoid)
                    return;
                else
                    throw new InvalidOperationException();
            }
        }

        private object processScallar(IOutput outp)
        {
            using (outp)
            {
                if (outp is OutputError)
                    throw (outp as OutputError).CreateException();
                else if (outp is OutputPrepared)
                    return (outp as OutputPrepared).QueryID;
                else if (outp is OutputSetKeyspace)
                    return (outp as OutputSetKeyspace).Value;
                else
                    throw new InvalidOperationException();
            }
        }


        private CqlRowSet processRowset(IOutput outp)
        {
            if (outp is OutputError)
            {
                try
                {
                    throw (outp as OutputError).CreateException();
                }
                finally
                {
                    outp.Dispose();
                }
            }
            else if (outp is OutputRows)
            {
                return new CqlRowSet(outp as OutputRows, true);
            }
            else
                throw new InvalidOperationException();
        }

        public IAsyncResult BeginNonQuery(string cqlQuery, AsyncCallback callback, object state, bool delayedRelease = false)
        {
            var c = new ConnectionWrapper() { connection = connect(), delayedRelease = delayedRelease };
            try
            {
                return c.connection.BeginQuery(cqlQuery, callback, state, c);
            }
            finally
            {
                if (!delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public void EndNonQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((Internal.AsyncResult<IOutput>)result).AsyncOwner;
            try
            {
                processNonQuery(c.connection.EndQuery(result, c));
            }
            finally
            {
                if (c.delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public void NonQuery(string cqlQuery)
        {
            var connection = connect();
            try
            {
                processNonQuery(connection.Query(cqlQuery));
            }
            finally
            {
                releaseConnection(connection);
            }
        }
        
        public IAsyncResult BeginScalar(string cqlQuery, AsyncCallback callback, object state, bool delayedRelease = false)
        {
            var c = new ConnectionWrapper() { connection = connect(), delayedRelease = delayedRelease };
            try
            {
                return c.connection.BeginQuery(cqlQuery, callback, state, c);
            }
            finally
            {
                if (!delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public object EndScalar(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((Internal.AsyncResult<IOutput>)result).AsyncOwner;
            try
            {
                return processScallar(c.connection.EndQuery(result, c));
            }
            finally
            {
                if (c.delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public object Scalar(string cqlQuery)
        {
            var connection = connect();
            try
            {
                return processScallar(connection.Query(cqlQuery));
            }
            finally
            {
                releaseConnection(connection);
            }
        }

        public IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, bool delayedRelease = false)
        {
            var c = new ConnectionWrapper() { connection = connect(), delayedRelease = delayedRelease };
            try
            {
                return c.connection.BeginQuery(cqlQuery, callback, state, c);
            }
            finally
            {
                if (!delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public CqlRowSet EndQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((Internal.AsyncResult<IOutput>)result).AsyncOwner;
            try
            {
                return processRowset(c.connection.EndQuery(result, c));
            }
            finally
            {
                if (c.delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public CqlRowSet Query(string cqlQuery)
        {
            var connection = connect();
            try
            {
                return processRowset(connection.Query(cqlQuery));
            }
            finally
            {
                releaseConnection(connection);
            }
        }

        public IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, bool delayedRelease = false)
        {
            var c = new ConnectionWrapper() { connection = connect(), delayedRelease = delayedRelease };
            try
            {
                return c.connection.BeginPrepareQuery(cqlQuery, callback, state, c);
            }
            finally
            {
                if (!delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public int EndPrepareQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((Internal.AsyncResult<IOutput>)result).AsyncOwner;
            try
            {
                return (int)processScallar(c.connection.EndPrepareQuery(result, c));
            }
            finally
            {
                if (c.delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public int PrepareQuery(string cqlQuery)
        {
            var connection = connect();
            try
            {
                return (int)processScallar(connection.PrepareQuery(cqlQuery));
            }
            finally
            {
                releaseConnection(connection);
            }
        }

        public IAsyncResult BeginExecuteQuery(int Id, Metadata Metadata, object[] values, AsyncCallback callback, object state, bool delayedRelease)
        {
            var c = new ConnectionWrapper() { connection = connect(), delayedRelease = delayedRelease };
            try
            {
                return c.connection.BeginExecuteQuery(Id, Metadata, values, callback, state, c);
            }
            finally
            {
                if (!delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public CqlRowSet EndExecuteQuery(IAsyncResult result)
        {
            var c = (ConnectionWrapper)((Internal.AsyncResult<IOutput>)result).AsyncOwner;
            try
            {
                return processRowset(c.connection.EndExecuteQuery(result, c));
            }
            finally
            {
                if (c.delayedRelease)
                    releaseConnection(c.connection);
            }
        }

        public CqlRowSet ExecuteQuery(int Id, Metadata Metadata, object[] values)
        {
            var connection = connect();
            try
            {
                return processRowset(connection.ExecuteQuery(Id, Metadata, values));
            }
            finally
            {
                releaseConnection(connection);
            }
        }
    }
}
