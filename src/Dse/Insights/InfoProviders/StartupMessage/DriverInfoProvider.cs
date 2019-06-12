//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Helpers;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class DriverInfoProvider : IInsightsInfoProvider<DriverInfo>
    {
        public DriverInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            return new DriverInfo
            {
                DriverName = AssemblyHelpers.GetAssemblyTitle(typeof(DriverInfoProvider)),
                DriverVersion = AssemblyHelpers.GetAssemblyInformationalVersion(typeof(DriverInfoProvider))
            };
        }
    }
}