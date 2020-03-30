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

using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace Cassandra.Helpers
{
    internal static class PlatformHelper
    {
        private static readonly Logger Logger = new Logger(typeof(PlatformHelper));

        public static bool IsKerberosSupported()
        {
#if NETFRAMEWORK
            return true;
#else
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        }

        public static string GetTargetFramework()
        {
#if NET452
            return ".NET Framework 4.5.2";
#elif NETSTANDARD2_0
            return ".NET Standard 2.0";
#else
            return null;
#endif
        }

        public static CpuInfo GetCpuInfo()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    return PlatformHelper.GetWmiCpuInfo();
                }

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    return PlatformHelper.GetLinuxProcCpuInfo();
                }
            }
            catch (Exception ex)
            {
                PlatformHelper.Logger.Info("Could not get cpu name. Exception: {0}", ex.ToString());
            }

            return new CpuInfo(null, Environment.ProcessorCount);
        }

        internal class CpuInfo
        {
            public string FirstCpuName { get; }

            public int Length { get; }

            public CpuInfo(string name, int length)
            {
                FirstCpuName = name;
                Length = length;
            }
        }
        
        public static CpuInfo GetWmiCpuInfo()
        {
            var count = 0;
            var first = true;
            var firstCpuName = string.Empty;
            foreach (var item in new System.Management.ManagementObjectSearcher("Select * from Win32_Processor").Get())
            {
                if (first)
                {
                    first = false;
                    firstCpuName = item["Name"].ToString();
                }

                int.TryParse(item["NumberOfCores"].ToString(), out var numberOfCores);

                count += numberOfCores;
            }

            return new CpuInfo(firstCpuName, count);
        }

        public static CpuInfo GetLinuxProcCpuInfo()
        {
            var cpuInfoLines = File.ReadAllLines(@"/proc/cpuinfo");
            var regex = new Regex(@"^model name\s+:\s+(.+)", RegexOptions.Compiled);
            foreach (string cpuInfoLine in cpuInfoLines)
            {
                var match = regex.Match(cpuInfoLine);
                if (match.Groups[0].Success)
                {
                    return new CpuInfo(match.Groups[1].Value, Environment.ProcessorCount);
                }
            }

            return new CpuInfo(null, Environment.ProcessorCount);
        }

#if !NETFRAMEWORK
        public static bool RuntimeSupportsCloudTlsSettings()
        {
            var netCoreVersion = PlatformHelper.GetNetCoreVersion();
            if (netCoreVersion != null && Version.TryParse(netCoreVersion.Split('-')[0], out var version))
            {
                PlatformHelper.Logger.Info("NET Core Runtime detected: " + netCoreVersion);
                if (version.Major > 2)
                {
                    return true;
                }

                if (version.Major == 2 && version.Minor >= 1)
                {
                    return true;
                }

                return false;
            }

            PlatformHelper.Logger.Warning(
                "Could not detect NET Core Runtime or version parsing failed. " +
                "Assuming that HttpClientHandler supports the required TLS settings for Cloud. " +
                "Version: " + (netCoreVersion ?? "null"));

            // if we can't detect the version, assume it supports
            return true;
        }

        public static string GetNetCoreVersion()
        {
            var assembly = typeof(System.Runtime.GCSettings).GetTypeInfo().Assembly;
            var assemblyPath = assembly.CodeBase.Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries);
            var netCoreAppIndex = Array.IndexOf(assemblyPath, "Microsoft.NETCore.App");
            if (netCoreAppIndex > 0 && netCoreAppIndex < assemblyPath.Length - 2)
            {
                return assemblyPath[netCoreAppIndex + 1];
            }
            return null;
        }
#endif
    }
}