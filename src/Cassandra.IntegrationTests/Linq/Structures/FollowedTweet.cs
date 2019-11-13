//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Cassandra.Data.Linq;
using System.Diagnostics;
#pragma warning disable 618

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
