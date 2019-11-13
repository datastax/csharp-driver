//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.Insights.Schema.StartupMessage;

namespace Cassandra.Insights.InfoProviders.StartupMessage
{
    internal interface IPolicyInfoMapper<in T>
    {
        PolicyInfo GetPolicyInformation(T policy);
    }
}