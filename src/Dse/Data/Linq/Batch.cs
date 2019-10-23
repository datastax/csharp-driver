//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Dse.Metrics.Internal;
using Dse.SessionManagement;
using Dse.Tasks;

namespace Dse.Data.Linq
{
    public abstract class Batch : Statement
    {
        private readonly IMetricsManager _metricsManager;

        protected readonly ISession _session;

        protected BatchType _batchType;
        protected DateTimeOffset? _timestamp = null;

        protected int QueryAbortTimeout { get; private set; }

        public abstract bool IsEmpty { get; }

        public override RoutingKey RoutingKey
        {
            get { return null; }
        }

        public QueryTrace QueryTrace { get; private set; }

        /// <inheritdoc />
        public override string Keyspace => null;

        internal Batch(ISession session, BatchType batchType)
        {
            _session = session;
            _metricsManager = (session as IInternalSession)?.MetricsManager;
            _batchType = batchType;
            QueryAbortTimeout = session.Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout;
        }

        public abstract void Append(CqlCommand cqlCommand);

        public new Batch SetConsistencyLevel(ConsistencyLevel? consistencyLevel)
        {
            base.SetConsistencyLevel(consistencyLevel);
            return this;
        }

        public new Batch SetTimestamp(DateTimeOffset timestamp)
        {
            _timestamp = timestamp;
            return this;
        }
        
        public Batch Append(IEnumerable<CqlCommand> cqlCommands)
        {
            foreach (var cmd in cqlCommands)
            {
                Append(cmd);
            }
            return this;
        }

        public void Execute()
        {
            Execute(Configuration.DefaultExecutionProfileName);
        }
        
        public void Execute(string executionProfile)
        {
            WaitToCompleteWithMetrics(InternalExecuteAsync(executionProfile), QueryAbortTimeout);
        }

        protected abstract Task<RowSet> InternalExecuteAsync();
        
        protected abstract Task<RowSet> InternalExecuteAsync(string executionProfile);
        
        public Task ExecuteAsync()
        {
            return InternalExecuteAsync();
        }
        
        public Task ExecuteAsync(string executionProfile)
        {
            return InternalExecuteAsync(executionProfile);
        }

        public IAsyncResult BeginExecute(AsyncCallback callback, object state)
        {
            return InternalExecuteAsync().ToApm(callback, state);
        }

        public void EndExecute(IAsyncResult ar)
        {
            var task = (Task)ar;
            task.Wait();
        }

        protected string BatchTypeString()
        {
            switch (_batchType)
            {
                case BatchType.Counter: return "COUNTER ";
                case BatchType.Unlogged: return "UNLOGGED ";
                case BatchType.Logged:
                    return "";
                default:
                    throw new ArgumentException();
            }
        }

        internal T WaitToCompleteWithMetrics<T>(Task<T> task, int timeout = Timeout.Infinite)
        {
            return TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, timeout);
        }
    }
}
