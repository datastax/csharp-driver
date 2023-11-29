using System.Collections.Concurrent;

namespace Cassandra
{
    public class RequestTrackingInfo
    {
        public RequestTrackingInfo()
        {
            this.Items = new ConcurrentDictionary<string, object>();
        }

        public ConcurrentDictionary<string, object> Items { get; }

        public IStatement Statement { get; set; }
    }
}
