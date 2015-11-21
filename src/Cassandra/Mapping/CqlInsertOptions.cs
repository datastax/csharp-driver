using System;
using System.Collections.Generic;
using LinqCqlQueryTools = Cassandra.Data.Linq.CqlQueryTools;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Represents insert options available on a per-query basis.
    /// </summary>
    public class CqlInsertOptions
    {
        public TimeSpan? Ttl { get; set; }
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// Creates a new instance of CqlInsertOptions.
        /// </summary>
        public static CqlInsertOptions New()
        {
            return new CqlInsertOptions();
        }

        public CqlInsertOptions SetTtl(TimeSpan ttl)
        {
            Ttl = ttl;
            return this;
        }

        public CqlInsertOptions SetTimestamp(DateTimeOffset timestamp)
        {
            Timestamp = timestamp;
            return this;
        }

        internal string GetCql()
        {
            var options = string.Join(" AND ", GetOptionsCql());
            if (string.IsNullOrEmpty(options))
                return null;

            return string.Format(" USING {0}", options);
        }

        private IEnumerable<string> GetOptionsCql()
        {
            if (Ttl != null) yield return string.Format("TTL {0}", (long)Ttl.Value.TotalSeconds);
            if (Timestamp != null)
                yield return string.Format("TIMESTAMP {0}", (Timestamp.Value - LinqCqlQueryTools.UnixStart).Ticks/10);
        }
    }
}