//
//  Copyright (C) 2017 DataStax, Inc.
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
    public class CassandraLog
    {
        [PartitionKey] public string category;

        [ClusteringKey(0)] public DateTimeOffset date;

        [ClusteringKey(1)] public string message;

        public void display()
        {
            Trace.TraceInformation(category + "\n " + date.ToString("MM/dd/yyyy H:mm:ss.fff zzz") + "\n " + message
                              + Environment.NewLine);
        }
    }
}
