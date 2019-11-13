//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.Data.Linq;
#pragma warning disable 618

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class Statistics
    {
        [PartitionKey] public string author_id;

        [Counter] public long followers_count;
        [Counter] public long tweets_count;
    }
}
