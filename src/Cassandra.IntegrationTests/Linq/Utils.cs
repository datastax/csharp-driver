//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

﻿using System;
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