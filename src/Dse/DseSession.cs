//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Dse;
using Dse.Graph;
using Dse.Tasks;

namespace Dse
{
    internal class DseSession : IDseSession
    {
        private static readonly Logger Logger = new Logger(typeof(IDseSession));
        private readonly ISession _coreSession;
        private readonly DseConfiguration _config;
        private readonly ICluster _cluster;

        public int BinaryProtocolVersion
        {
            get { return _coreSession.BinaryProtocolVersion; }
        }

        public ICluster Cluster
        {
            get { return _cluster; }
        }

        public bool IsDisposed
        {
            get { return _coreSession.IsDisposed; }
        }

        public string Keyspace
        {
            get { return _coreSession.Keyspace; }
        }

        public UdtMappingDefinitions UserDefinedTypes
        {
            get { return _coreSession.UserDefinedTypes; }
        }

        internal DseSession(ISession coreSession, DseConfiguration config) : this(coreSession, coreSession.Cluster, config)
        {
            
        }

        public DseSession(ISession coreSession, ICluster cluster, DseConfiguration config)
        {
            if (coreSession == null)
            {
                throw new ArgumentNullException("coreSession");
            }
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            _cluster = cluster;
            _coreSession = coreSession;
            _config = config;
        }

        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            return TaskHelper.WaitToComplete(ExecuteGraphAsync(statement));
        }

        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement)
        {
            var stmt = graphStatement.ToIStatement(_config.GraphOptions);
            return GetAnalyticsMaster(stmt, graphStatement)
                .Then(s =>
                    _coreSession
                        .ExecuteAsync(s)
                        .ContinueSync(rs => new GraphResultSet(rs)));
        }

        public Task<IStatement> GetAnalyticsMaster(IStatement statement, IGraphStatement graphStatement)
        {
            if (!(statement is TargettedSimpleStatement) || !_config.GraphOptions.IsAnalyticsQuery(graphStatement))
            {
                return TaskHelper.ToTask(statement);
            }
            var targettedSimpleStatement = (TargettedSimpleStatement) statement;
            return _coreSession
                .ExecuteAsync(new SimpleStatement("CALL DseClientTool.getAnalyticsGraphServer()"))
                .ContinueWith(t => AdaptRpcMasterResult(t, targettedSimpleStatement), TaskContinuationOptions.ExecuteSynchronously);
        }

        private IStatement AdaptRpcMasterResult(Task<RowSet> task, TargettedSimpleStatement statement)
        {
            if (task.IsFaulted)
            {
                Logger.Verbose("Error querying graph analytics server, query will not be routed optimally: {0}", task.Exception);
                return statement;
            }
            var row = task.Result.FirstOrDefault();
            if (row == null)
            {
                Logger.Verbose("Empty response querying graph analytics server, query will not be routed optimally");
                return statement;
            }
            var resultField = row.GetValue<IDictionary<string, string>>("result");
            if (resultField == null || !resultField.ContainsKey("location") || resultField["location"] == null)
            {
                Logger.Verbose("Could not extract graph analytics server location from RPC, query will not be routed optimally");
                return statement;
            }
            var location = resultField["location"];
            var hostName = location.Substring(0, location.LastIndexOf(':'));
            var address = _config.AddressTranslator.Translate(
                new IPEndPoint(IPAddress.Parse(hostName),_config.CassandraConfiguration.ProtocolOptions.Port));
            var host = _coreSession.Cluster.GetHost(address);
            statement.PreferredHost = host;
            return statement;
        }

        public void Dispose()
        {
            _coreSession.Dispose();
        }

        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return _coreSession.BeginExecute(statement, callback, state);
        }

        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return _coreSession.BeginExecute(cqlQuery, consistency, callback, state);
        }

        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return _coreSession.BeginPrepare(cqlQuery, callback, state);
        }

        public void ChangeKeyspace(string keyspaceName)
        {
            _coreSession.ChangeKeyspace(keyspaceName);
        }

        public void CreateKeyspace(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            _coreSession.CreateKeyspace(keyspaceName, replication, durableWrites);
        }

        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            _coreSession.CreateKeyspaceIfNotExists(keyspaceName, replication, durableWrites);
        }

        public void DeleteKeyspace(string keyspaceName)
        {
            _coreSession.DeleteKeyspace(keyspaceName);
        }

        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            _coreSession.DeleteKeyspaceIfExists(keyspaceName);
        }

        public RowSet EndExecute(IAsyncResult ar)
        {
            return _coreSession.EndExecute(ar);
        }

        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            return _coreSession.EndPrepare(ar);
        }

        public RowSet Execute(IStatement statement)
        {
            return _coreSession.Execute(statement);
        }

        public RowSet Execute(string cqlQuery)
        {
            return _coreSession.Execute(cqlQuery);
        }

        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return _coreSession.Execute(cqlQuery, consistency);
        }

        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return _coreSession.Execute(cqlQuery, pageSize);
        }

        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return _coreSession.ExecuteAsync(statement);
        }

        public PreparedStatement Prepare(string cqlQuery)
        {
            return _coreSession.Prepare(cqlQuery);
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.Prepare(cqlQuery, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery)
        {
            return _coreSession.PrepareAsync(cqlQuery);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.PrepareAsync(cqlQuery, customPayload);
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
            #pragma warning disable 618
            _coreSession.WaitForSchemaAgreement(rs);
            #pragma warning restore 618
        }

        public bool WaitForSchemaAgreement(IPEndPoint forHost)
        {
            #pragma warning disable 618
            return _coreSession.WaitForSchemaAgreement(forHost);
            #pragma warning restore 618
        }
    }
}
