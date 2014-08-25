using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

namespace Cassandra
{
    internal class Hosts
    {
        private readonly ConcurrentDictionary<IPEndPoint, Host> _hosts = new ConcurrentDictionary<IPEndPoint, Host>();
        private readonly IReconnectionPolicy _rp;

        public Hosts(IReconnectionPolicy rp)
        {
            _rp = rp;
        }

        public bool TryGet(IPEndPoint endpoint, out Host host)
        {
            return _hosts.TryGetValue(endpoint, out host);
        }

        public ICollection<Host> ToCollection()
        {
            return new List<Host>(_hosts.Values);
        }

        public bool AddIfNotExistsOrBringUpIfDown(IPEndPoint ep)
        {
            if (!_hosts.ContainsKey(ep))
                if (_hosts.TryAdd(ep, new Host(ep, _rp)))
                    return true;

            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.BringUpIfDown();
            return false;
        }

        public bool SetDownIfExists(IPEndPoint ep)
        {
            Host host;
            if (_hosts.TryGetValue(ep, out host))
                return host.SetDown();
            return false;
        }

        public void RemoveIfExists(IPEndPoint ep)
        {
            Host host;
            _hosts.TryRemove(ep, out host);
        }

        public IEnumerable<IPEndPoint> AllEndPointsToCollection()
        {
            return new List<IPEndPoint>(_hosts.Keys);
        }
    }
}