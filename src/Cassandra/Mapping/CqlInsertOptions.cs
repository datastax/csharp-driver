using System.Collections.Generic;

namespace Cassandra.Mapping
{
    public class CqlInsertOptions
    {
        public int? Ttl { get; set; }
        public int? Timestamp { get; set; }

        public CqlInsertOptions SetTtl(int ttl)
        {
            Ttl = ttl;
            return this;
        }

        public CqlInsertOptions DisableTtl()
        {
            Ttl = null;
            return this;
        }
        public CqlInsertOptions SetTimestamp(int timestamp)
        {
            Timestamp = timestamp;
            return this;
        }

        public CqlInsertOptions DisableTimestamp()
        {
            Timestamp = null;
            return this;
        }

        internal string GenerateQueryOptions()
        {
            var options = string.Join(" AND ", GetOptions());
            if (string.IsNullOrEmpty(options))
                return null;

            return string.Format(" USING {0}", options);
        }

        private IEnumerable<string> GetOptions()
        {
            if (Ttl != null) yield return string.Format("TTL {0}", Ttl);
            if (Timestamp != null) yield return string.Format("TIMESTAMP {0}", Timestamp);
        }
    }
}