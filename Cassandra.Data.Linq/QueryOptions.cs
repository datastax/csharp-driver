using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    public abstract class QueryOptions
    {
        public long Timestamp { get; set; }

        public string GetCql()
        {
            List<string> options = new List<string>();
            PopulateOptions(options);
            if (!options.Any())
            {
                return string.Empty;
            }

            StringBuilder sb = new StringBuilder();
            sb.Append(" USING ");
            sb.Append(string.Join(" AND ", options));
            return sb.ToString();
        }

        protected virtual void PopulateOptions(List<string> opts)
        {
            if (Timestamp > 0)
            {
                opts.Add(string.Format("TIMESTAMP {0}", Timestamp));
            }
        }
    }

    public class InsertOptions : QueryOptions
    {
        public int TimeToLive { get; set; }

        protected override void PopulateOptions(List<string> opts)
        {
            base.PopulateOptions(opts);
            if (TimeToLive > 0)
            {
                opts.Add(string.Format("TTL {0}", Timestamp));
            }
        }
    }

    public class UpdateOptions : QueryOptions
    {
        public int TimeToLive { get; set; }

        protected override void PopulateOptions(List<string> opts)
        {
            base.PopulateOptions(opts);
            if (TimeToLive > 0)
            {
                opts.Add(string.Format("TTL {0}", Timestamp));
            }
        }
    }

    public class DeleteOptions : QueryOptions
    {
    }
}