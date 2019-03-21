//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Dse.Helpers;
using Dse.Insights.Schema.StartupMessage;
using Dse.SessionManagement;

namespace Dse.Insights.InfoProviders.StartupMessage
{
    internal class PlatformInfoProvider : IInsightsInfoProvider<InsightsPlatformInfo>
    {
        public InsightsPlatformInfo GetInformation(IInternalDseCluster cluster, IInternalDseSession dseSession)
        {
            var cpuInfo = PlatformHelper.GetCpuInfo();
            var dependencies = typeof(PlatformInfoProvider).GetTypeInfo().Assembly.GetReferencedAssemblies().Select(name =>
                new AssemblyInfo
                {
                    FullName = name.FullName,
                    Name = name.Name,
                    Version = name.Version?.ToString()
                });

            var dependenciesDictionary = new Dictionary<string, AssemblyInfo>();
            foreach (var p in dependencies)
            {
                // Use the first value in list
                if (!dependenciesDictionary.ContainsKey(p.Name))
                {
                    dependenciesDictionary[p.Name] = p;
                }
            }

            return new InsightsPlatformInfo
            {
                CentralProcessingUnits = new CentralProcessingUnitsInfo
                {
                    Length = cpuInfo.Length,
                    Model = cpuInfo.FirstCpuName
                },
                OperatingSystem = new OperatingSystemInfo
                {
                    Name = RuntimeInformation.OSDescription,
                    Version = RuntimeInformation.OSDescription,
                    Arch = RuntimeInformation.OSArchitecture.ToString()
                },
                Runtime = new RuntimeInfo
                {
                    Dependencies = dependenciesDictionary,
                    RuntimeFramework = RuntimeInformation.FrameworkDescription,
                    TargetFramework = PlatformHelper.GetTargetFramework(),
                }
            };
        }
    }
}