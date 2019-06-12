// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Collections.Generic;
using Cassandra.Helpers;

namespace Cassandra.Requests
{
    internal class StartupOptionsFactory : IStartupOptionsFactory
    {
        public const string CqlVersionOption = "CQL_VERSION";
        public const string CompressionOption = "COMPRESSION";
        public const string NoCompactOption = "NO_COMPACT";
        public const string DriverNameOption = "DRIVER_NAME";
        public const string DriverVersionOption = "DRIVER_VERSION";

        public const string CqlVersion = "3.0.0";
        public const string SnappyCompression = "snappy";
        public const string Lz4Compression = "lz4";

        public IReadOnlyDictionary<string, string> CreateStartupOptions(ProtocolOptions options)
        {
            var startupOptions = new Dictionary<string, string>
            {
                { StartupOptionsFactory.CqlVersionOption, StartupOptionsFactory.CqlVersion }
            };

            string compressionName = null;
            switch (options.Compression)
            {
                case CompressionType.LZ4:
                    compressionName = StartupOptionsFactory.Lz4Compression;
                    break;
                case CompressionType.Snappy:
                    compressionName = StartupOptionsFactory.SnappyCompression;
                    break;
            }

            if (compressionName != null)
            {
                startupOptions.Add(StartupOptionsFactory.CompressionOption, compressionName);
            }

            if (options.NoCompact)
            {
                startupOptions.Add(StartupOptionsFactory.NoCompactOption, "true");
            }

            startupOptions.Add(StartupOptionsFactory.DriverNameOption, AssemblyHelpers.GetAssemblyTitle(typeof(StartupOptionsFactory)));
            startupOptions.Add(
                StartupOptionsFactory.DriverVersionOption, AssemblyHelpers.GetAssemblyInformationalVersion(typeof(StartupOptionsFactory)));

            return startupOptions;
        }
    }
}