using System;
using Cassandra.Data.Linq;

namespace Playground
{
    public static class Utils
    {
        public static void DisplayTable(this Table<Statistics> table)
        {            
            Console.WriteLine("┌──────────┬────────────────┬─────────────┐");
            Console.WriteLine(String.Format("│{0,10}│{1,16}│{2,13}│", "Author ID", "Followers count", "Tweets count"));
            Console.WriteLine("├──────────┼────────────────┼─────────────┤");
            
            foreach (var stat in (from st in table select st).Execute())
                Console.WriteLine(String.Format("│{0,10}│{1,16}│{2,13}│", stat.author_id, stat.followers_count, stat.tweets_count));

            Console.WriteLine("└──────────┴────────────────┴─────────────┘");
        }
    }
}
