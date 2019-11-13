// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using Cassandra.SessionManagement;

namespace Cassandra.Insights
{
    internal interface IInsightsSupportVerifier
    {
        bool SupportsInsights(IInternalDseCluster cluster);

        bool DseVersionSupportsInsights(Version dseVersion);
    }
}