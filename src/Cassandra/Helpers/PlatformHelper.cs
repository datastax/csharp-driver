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
using System.Reflection;

namespace Cassandra.Helpers
{
    internal static class PlatformHelper
    {
        private static readonly Logger Logger = new Logger(typeof(PlatformHelper));

#if !NET45
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