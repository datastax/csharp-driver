//
//      Copyright (C) 2012 DataStax Inc.
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

using System;
using Cassandra.Data.Linq;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class FollowedTweet
    {
        public string author_id;

        public string body;
        [SecondaryIndex] public DateTimeOffset date;
        [ClusteringKey(0)] public Guid tweet_id;
        [PartitionKey] public string user_id;

        public void display()
        {
            Trace.TraceInformation("Author: " + author_id);
            Trace.TraceInformation("Date: " + date);
            Trace.TraceInformation("Tweet content: " + body + Environment.NewLine);
        }
    }
}