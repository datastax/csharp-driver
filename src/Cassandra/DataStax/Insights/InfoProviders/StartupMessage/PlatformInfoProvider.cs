//
//      Copyright (C) DataStax Inc.
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

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.Helpers;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.InfoProviders.StartupMessage
{
    internal class PlatformInfoProvider : IInsightsInfoProvider<InsightsPlatformInfo>
    {
        public InsightsPlatformInfo GetInformation(IInternalCluster cluster, IInternalSession session)
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