using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace Dse.Helpers
{
    internal static class PlatformHelper
    {
        private static readonly Logger Logger = new Logger(typeof(PlatformHelper));

        public static bool IsKerberosSupported()
        {
#if NET45
            return true;
#elif NETCORE
            return false;
#else
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        }

        public static string GetTargetFramework()
        {
#if NETSTANDARD1_5
            return ".NET Standard 1.5";
#elif NET45
            return ".NET Framework 4.5";
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
#if !NETSTANDARD1_5
                    return PlatformHelper.GetWmiCpuInfo();
#else
                    PlatformHelper.Logger.Info("GetCpuInfo is not supported on Windows when targeting NETStandard 1.5");
#endif
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

#if !NETSTANDARD1_5
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
#endif

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
    }
}