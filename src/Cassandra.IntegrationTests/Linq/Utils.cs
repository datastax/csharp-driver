﻿using System;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;

namespace Cassandra.IntegrationTests.Linq
{
    public static class Utils
    {
        public static void DisplayTable(this Table<Statistics> table)
        {
            Console.WriteLine("┌──────────┬────────────────┬─────────────┐");
            Console.WriteLine("│{0,10}│{1,16}│{2,13}│", "Author ID", "Followers count", "Tweets count");
            Console.WriteLine("├──────────┼────────────────┼─────────────┤");

            foreach (Statistics stat in (from st in table select st).Execute())
                Console.WriteLine("│{0,10}│{1,16}│{2,13}│", stat.author_id, stat.followers_count, stat.tweets_count);

            Console.WriteLine("└──────────┴────────────────┴─────────────┘");
        }
    }
}