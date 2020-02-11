//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Dse.Connections;
using Dse.ExecutionProfiles;
using Dse.Graph;
using Dse.Metrics;
using Dse.Metrics.Internal;
using Dse.Observers.Abstractions;
using Dse.SessionManagement;
using Dse.Tasks;

namespace Dse
{
    internal class DseSession : IInternalDseSession
    {
        private static readonly Logger Logger = new Logger(typeof(IDseSession));
        private readonly IInternalSession _coreSession;
        private readonly DseConfiguration _config;
        private readonly ISessionManager _dseSessionManager;
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

        public Task Init(ISessionManager sessionManager)
        {
            return _coreSession.Init(sessionManager);
        }

        public Task Init()
        {
            return _coreSession.Init(_dseSessionManager);
        }

        public IHostConnectionPool GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            return _coreSession.GetOrCreateConnectionPool(host, distance);
        }

        IEnumerable<KeyValuePair<IPEndPoint, IHostConnectionPool>> IInternalSession.GetPools()
        {
            return _coreSession.GetPools();
        }

        public IHostConnectionPool GetExistingPool(IPEndPoint address)
        {
            return _coreSession.GetExistingPool(address);
        }

        public void CheckHealth(Host host, IConnection connection)
        {
            _coreSession.CheckHealth(host, connection);
        }

        public bool HasConnections(Host host)
        {
            return _coreSession.HasConnections(host);
        }
        
        public void OnAllConnectionClosed(Host host, IHostConnectionPool pool)
        {
            _coreSession.OnAllConnectionClosed(host, pool);
        }

        string IInternalSession.Keyspace
        {
            get => _coreSession.Keyspace;
            set => _coreSession.Keyspace = value;
        }

        public Configuration Configuration => _coreSession.Cluster.Configuration;

        public IInternalCluster InternalCluster => _coreSession.InternalCluster;

        public string Keyspace
        {
            get { return _coreSession.Keyspace; }
        }

        public UdtMappingDefinitions UserDefinedTypes
        {
            get { return _coreSession.UserDefinedTypes; }
        }

        public string SessionName => _coreSession.SessionName;

        public DseSession(IInternalSession coreSession, IInternalDseCluster cluster)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            _coreSession = coreSession ?? throw new ArgumentNullException(nameof(coreSession));
            _config = cluster.Configuration ?? throw new ArgumentNullException(nameof(cluster.Configuration));
            _dseSessionManager = cluster.Configuration.DseSessionManagerFactory.Create(cluster, this);
            InternalSessionId = Guid.NewGuid();
        }

        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            return ExecuteGraph(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement)
        {
            return ExecuteGraphAsync(graphStatement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName)
        {
            return TaskHelper.WaitToCompleteWithMetrics(MetricsManager, ExecuteGraphAsync(statement, executionProfileName));
        }

        /// <inheritdoc />
        public async Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement, string executionProfileName)
        {
            var requestOptions = _coreSession.GetRequestOptions(executionProfileName);
            var stmt = graphStatement.ToIStatement(requestOptions.GraphOptions);
            await GetAnalyticsMaster(stmt, graphStatement, requestOptions).ConfigureAwait(false);
            var rs = await _coreSession.ExecuteAsync(stmt, requestOptions).ConfigureAwait(false);
            return GraphResultSet.CreateNew(rs, graphStatement, requestOptions.GraphOptions);
        }

        private async Task<IStatement> GetAnalyticsMaster(
            IStatement statement, IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            if (!(statement is TargettedSimpleStatement) || !requestOptions.GraphOptions.IsAnalyticsQuery(graphStatement))
            {
                return statement;
            }

            var targettedSimpleStatement = (TargettedSimpleStatement)statement;

            RowSet rs;
            try
            {
                rs = await _coreSession.ExecuteAsync(
                    new SimpleStatement("CALL DseClientTool.getAnalyticsGraphServer()"), requestOptions).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.Verbose("Error querying graph analytics server, query will not be routed optimally: {0}", ex);
                return statement;
            }

            return AdaptRpcMasterResult(rs, targettedSimpleStatement);
        }

        private IStatement AdaptRpcMasterResult(RowSet rowSet, TargettedSimpleStatement statement)
        {
            var row = rowSet.FirstOrDefault();
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
                new IPEndPoint(IPAddress.Parse(hostName), _config.CassandraConfiguration.ProtocolOptions.Port));
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

        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            return _coreSession.Execute(statement, executionProfileName);
        }

        public RowSet Execute(IStatement statement)
        {
            return _coreSession.Execute(statement);
        }

        public RowSet Execute(string cqlQuery)
        {
            return _coreSession.Execute(cqlQuery);
        }

        public RowSet Execute(string cqlQuery, string executionProfileName)
        {
            return _coreSession.Execute(cqlQuery, executionProfileName);
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

        public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)
        {
            return _coreSession.ExecuteAsync(statement, executionProfileName);
        }

        public Task<RowSet> ExecuteAsync(IStatement statement, IRequestOptions requestOptions)
        {
            return _coreSession.ExecuteAsync(statement, requestOptions);
        }

        public IRequestOptions GetRequestOptions(string executionProfileName)
        {
            return _coreSession.GetRequestOptions(executionProfileName);
        }

        public int ConnectedNodes => _coreSession.ConnectedNodes;

        public IMetricsManager MetricsManager => _coreSession.MetricsManager;

        public IObserverFactory ObserverFactory => _coreSession.ObserverFactory;

        public PreparedStatement Prepare(string cqlQuery)
        {
            return _coreSession.Prepare(cqlQuery);
        }

        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.Prepare(cqlQuery, customPayload);
        }

        public PreparedStatement Prepare(string cqlQuery, string keyspace)
        {
            return _coreSession.Prepare(cqlQuery, keyspace);
        }

        public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.Prepare(cqlQuery, keyspace, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery)
        {
            return _coreSession.PrepareAsync(cqlQuery);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.PrepareAsync(cqlQuery, customPayload);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace)
        {
            return _coreSession.PrepareAsync(cqlQuery, keyspace);
        }

        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace,
                                                    IDictionary<string, byte[]> customPayload)
        {
            return _coreSession.PrepareAsync(cqlQuery, keyspace, customPayload);
        }

        public IDriverMetrics GetMetrics()
        {
            return _coreSession.GetMetrics();
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

        /// <inheritdoc />
        public Guid InternalSessionId { get; }
    }
}