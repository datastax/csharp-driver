using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Data;
using Cassandra;
using System.Reflection;

namespace Playground
{
    public static class Utils
    {
        public static void DisplayTable(this CqlTable<Statistics> table)
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
