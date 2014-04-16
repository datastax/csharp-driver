using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Cassandra.RequestHandlers
{
    /// <summary>
    /// Represents a handler that can get an available connection, send a request and parses the response when received.
    /// </summary>
    abstract class RequestHandler
    {
        private readonly Logger _logger = new Logger(typeof(RequestHandler));
        public CassandraConnection Connection;
        public ConsistencyLevel? Consistency = null;
        public Statement Query;
        private IEnumerator<Host> _hostsIter = null;
        public IAsyncResult LongActionAc;
        public readonly Dictionary<IPAddress, List<Exception>> InnerExceptions = new Dictionary<IPAddress, List<Exception>>();
        public readonly List<IPAddress> TriedHosts = new List<IPAddress>();
        public int QueryRetries = 0;
        virtual public void Connect(Session owner, bool moveNext, out int streamId)
        {
            if (_hostsIter == null)
            {
                _hostsIter = owner.Policies.LoadBalancingPolicy.NewQueryPlan(Query).GetEnumerator();
                if (!_hostsIter.MoveNext())
                {
                    var ex = new NoHostAvailableException(new Dictionary<IPAddress, List<Exception>>());
                    _logger.Error(ex);
                    throw ex;
                }
            }
            else
            {
                if (moveNext)
                    if (!_hostsIter.MoveNext())
                    {
                        var ex = new NoHostAvailableException(InnerExceptions);
                        _logger.Error(ex);
                        throw ex;
                    }
            }

            Connection = owner.Connect(_hostsIter, TriedHosts, InnerExceptions, out streamId);
        }
        abstract public void Begin(Session owner, int steamId);
        abstract public void Process(Session owner, IAsyncResult ar, out object value);
        abstract public void Complete(Session owner, object value, Exception exc = null);
    }
}
