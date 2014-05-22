using System;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Linq
{
    public static class Utils
    {
        public static void DisplayTable(this Table<Statistics> table)
        {
            Trace.TraceInformation("┌──────────┬────────────────┬─────────────┐");
            Trace.TraceInformation("│{0,10}│{1,16}│{2,13}│", "Author ID", "Followers count", "Tweets count");
            Trace.TraceInformation("├──────────┼────────────────┼─────────────┤");

            foreach (Statistics stat in (from st in table select st).Execute())
                Trace.TraceInformation("│{0,10}│{1,16}│{2,13}│", stat.author_id, stat.followers_count, stat.tweets_count);

            Trace.TraceInformation("└──────────┴────────────────┴─────────────┘");
        }
    }
}