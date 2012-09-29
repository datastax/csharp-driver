using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class CassandraManagedConnection : ICassandraConnectionCQL
    {
        WeakReference<CassandraManager> manager;
        internal CassandraManagedConnection(CassandraManager manager, CassandraConnection connection)
        {
            this.manager = new WeakReference<CassandraManager>(manager);
            this.connection = connection;
        }

        private CassandraConnection connection;

        Wrapper<bool> alreadyDisposed = new Wrapper<bool>(false);

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraManagedConnection");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (!alreadyDisposed.Value)
                {
                    alreadyDisposed.Value = true;
                    if (manager.IsAlive)
                        manager.Value.ReleaseConnection(connection);
                }
            }
        }

        public IAsyncResult BeginExecuteQuery(string cqlQuery, AsyncCallback callback, object state)
        {
            return connection.BeginExecuteQuery(cqlQuery, callback, state);
        }

        public IOutput EndExecuteQuery(IAsyncResult result)
        {
            return connection.EndExecuteQuery(result);
        }

        public IOutput ExecuteQuery(string cqlQuery)
        {
            return connection.ExecuteQuery(cqlQuery);
        }

        public IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state)
        {
            return connection.BeginPrepareQuery(cqlQuery, callback, state);
        }

        public IOutput EndPrepareQuery(IAsyncResult result)
        {
            return connection.EndPrepareQuery(result);
        }

        public IOutput PrepareQuery(string cqlQuery)
        {
            return connection.PrepareQuery(cqlQuery);
        }

        public IAsyncResult BeginExecuteExecute(int Id, Metadata Metadata, object[] values, AsyncCallback callback, object state)
        {
            return connection.BeginExecuteExecute(Id, Metadata, values, callback, state);
        }

        public IOutput EndExecuteExecute(IAsyncResult result)
        {
            return connection.EndExecuteExecute(result);
        }

        public IOutput ExecuteExecute(int Id, Metadata Metadata, object[] values)
        {
            return connection.ExecuteExecute(Id, Metadata, values);
        }
    }
}
