//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Insights.Schema.StartupMessage;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal interface IPolicyInfoMapper<in T>
    {
        PolicyInfo GetPolicyInformation(T policy);
    }
}