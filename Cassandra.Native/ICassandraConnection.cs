using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public interface ICassandraConnectionCredentials : IDisposable
    {
        IAsyncResult BeginExecuteCredentials(IDictionary<string, string> credentials, AsyncCallback callback, object state);
        IOutput EndExecuteCredentials(IAsyncResult result);
        IOutput ExecuteCredentials(IDictionary<string, string> credentials);

    }

    public interface ICassandraConnectionCQL : IDisposable
    {
        IAsyncResult BeginExecuteQuery(string cqlQuery, AsyncCallback callback, object state);
        IOutput EndExecuteQuery(IAsyncResult result);
        IOutput ExecuteQuery(string cqlQuery);
        IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state);
        IOutput EndPrepareQuery(IAsyncResult result);
        IOutput PrepareQuery(string cqlQuery);
        IAsyncResult BeginExecuteExecute(int Id, Metadata Metadata, object[] values, AsyncCallback callback, object state);
        IOutput EndExecuteExecute(IAsyncResult result);
        IOutput ExecuteExecute(int Id, Metadata Metadata, object[] values);
    }

    public interface ICassandraConnectionOptions : IDisposable
    {
        IAsyncResult BeginExecuteOptions(AsyncCallback callback, object state);
        IOutput EndExecuteOptions(IAsyncResult result);
        IOutput ExecuteOptions();
    }

    public interface ICassandraConnectionEvents
    {
        IAsyncResult BeginRegisterForCassandraEvent(CassandraEventType eventTypes, AsyncCallback callback, object state);
        IOutput EndRegisterForCassandraEvent(IAsyncResult result);
        IOutput RegisterForCassandraEvent(CassandraEventType eventTypes);

        event CassandraEventHandler CassandraEvent;
    }

    public interface ICassandraConnection : ICassandraConnectionCQL, ICassandraConnectionCredentials, ICassandraConnectionOptions, ICassandraConnectionEvents
    {
    }
}
