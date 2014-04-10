using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    /// <summary>
    ///  Basic information on the execution of a query. <p> This provides the
    ///  following information on the execution of a (successful) query: </p> <ul> <li>The
    ///  list of Cassandra hosts tried in order (usually just one, unless a node has
    ///  been tried but was dead/in error or a timeout provoked a retry (which depends
    ///  on the RetryPolicy)).</li> <li>The consistency level achieved by the query
    ///  (usually the one asked, though some specific RetryPolicy may allow this to be
    ///  different).</li> <li>The query trace recorded by Cassandra if tracing had
    ///  been set for the query.</li> </ul>
    /// </summary>
    public class ExecutionInfo
    {
        private ConsistencyLevel _achievedConsistency = ConsistencyLevel.Any;
        private QueryTrace _queryTrace;
        private List<IPAddress> _tiedHosts;

        public List<IPAddress> TriedHosts
        {
            get { return _tiedHosts; }
        }

        public IPAddress QueriedHost
        {
            get { return _tiedHosts.Count > 0 ? _tiedHosts[_tiedHosts.Count - 1] : null; }
        }

        public QueryTrace QueryTrace
        {
            get { return _queryTrace; }
        }

        public ConsistencyLevel AchievedConsistency
        {
            get { return _achievedConsistency; }
        }

        internal void SetTriedHosts(List<IPAddress> triedHosts)
        {
            _tiedHosts = triedHosts;
        }

        internal void SetQueryTrace(QueryTrace queryTrace)
        {
            _queryTrace = queryTrace;
        }

        internal void SetAchievedConsistency(ConsistencyLevel achievedConsistency)
        {
            _achievedConsistency = achievedConsistency;
        }
    }
}